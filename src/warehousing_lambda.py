import awswrangler as wr
import  pg8000
import boto3
from botocore.exceptions import ClientError
import json
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

PROCESSED_BUCKET = "nc-crigglestone-processed-bucket"

def get_rds_secret() -> dict:

    secret_name = "warehouse-db-credentials"
    region_name = "eu-west-2"

    client = boto3.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
        secret = json.loads(get_secret_value_response['SecretString'])
        return secret
    except ClientError as e:
        raise e 

def connect_to_warehouse():
    database_info = get_rds_secret()
    
    try:
        conn = pg8000.connect(
            host=database_info['host'],
            port=int(database_info['port']),
            database=database_info.get('database', 'warehouse'),
            user=database_info.get('username') or database_info.get('user'),
            password=database_info['password']
        )
        logger.info("Successfully connected to RDS warehouse.")
        return conn
    except Exception as e:
        logger.warning(f'Database connection failed due to {e}')
        raise

def load_parquet_to_warehouse(key):
    table_name = (
        key.replace("processed-dim-", "")
        .replace("processed-fact-", "")
        .replace(".parquet", "")
    )

    logger.info(f"Loading file {key} into table {table_name}")
    
    s3_path = f"s3://{PROCESSED_BUCKET}/{key}"
    processed_data = wr.s3.read_parquet(s3_path)

    # Remove extra columns that don't exist in the database
    columns_to_remove = ['date', '__index_level_0__']
    for col in columns_to_remove:
        if col in processed_data.columns:
            logger.info(f"Removing extra column: {col}")
            processed_data = processed_data.drop(columns=[col])

    if processed_data.empty:
            logger.info(f"No data found in {table_name}")
            return

    connection = connect_to_warehouse()

    try:
        wr.postgresql.to_sql(
            df=processed_data,
            table=table_name,
            con=connection,
            schema="public",        
            mode="append",     
            chunksize=1000   #Loads rows into batches      
        )

        logger.info(f"Loaded {len(processed_data)} rows into {table_name}")

    except Exception as e:
        logger.error(f"Failed to load new {table_name} rows into {table_name}: {e}")
        raise

# Logs the first 10 rows of every table in the 'public' schema of the warehouse
def preview_all_tables():

    connection = connect_to_warehouse()
    
    try:
        # Get list of all tables in the public schema
        tables_df = wr.postgresql.read_sql_query(
            sql="SELECT table_name FROM information_schema.tables WHERE table_schema='public';",
            con=connection
        )
        table_names = tables_df['table_name'].tolist()
        
        if not table_names:
            logger.info("No tables found in the warehouse.")
            return

        for table in table_names:
            logger.info(f"Previewing first 10 rows of table: {table}")            
            
            df_preview = wr.postgresql.read_sql_query(
                sql=f'SELECT * FROM "{table}" LIMIT 10;',
                con=connection
            )
            if df_preview.empty:
                logger.info(f"No data in table {table}")
            else:
                logger.info(f"\n{df_preview.to_string(index=False)}")
            #Visual break for the logs
            logger.info("-" * 50)
        
        # #Saving the warehouse extract
        # for table in table_names:
        #     df = wr.postgresql.read_sql_query(sql=f'SELECT * FROM "{table}"', con=connection)
        #     s3_path = f"s3://nc-crigglestone-lambda-bucket/extracts/{table}.csv"
        #     wr.s3.to_csv(df, path=s3_path, index=False)

    except Exception as e:
        logger.error(f"Failed to preview tables: {e}")
        raise
    finally:
        connection.close()

def check_date_table_schema(table_name):
    """Check what columns exist in the date table"""
    connection = connect_to_warehouse()
    
    try:
        schema_query = """
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = '{table_name}' AND table_schema = 'public'
        ORDER BY ordinal_position;
        """
        
        schema_df = wr.postgresql.read_sql_query(schema_query, connection)
        logger.info("Date table columns in database:")
        logger.info(f"\n{schema_df.to_string()}")
        
        return schema_df['column_name'].tolist()
        
    except Exception as e:
        logger.error(f"Failed to check table schema: {e}")
        raise
    finally:
        connection.close()

def debug_parquet_data(key):
    s3_path = f"s3://{PROCESSED_BUCKET}/{key}"
    
    try:
        # Read the data
        df = wr.s3.read_parquet(s3_path)
        
        logger.info(f"Data types in parquet file:")
        for col, dtype in df.dtypes.items():
            logger.info(f"  {col}: {dtype}")
        
        # Check what values are in date_id column
        logger.info(f"Unique values in date_id: {df['date_id'].unique()}")
        logger.info(f"Sample date_id values: {df['date_id'].head(10).tolist()}")
        
        # Check for non-numeric values
        non_numeric = df[~df['date_id'].apply(lambda x: isinstance(x, (int, float, type(None))))]
        if not non_numeric.empty:
            logger.warning(f"Non-numeric values in date_id: {non_numeric['date_id'].unique()}")
        
    except Exception as e:
        logger.error(f"Debug failed: {e}")
        raise  

def lambda_handler(event, context):
    logger.info("Warehouse loader started")
    try:
        debug_parquet_data("processed-dim-date.parquet")
        
        if 'Records' in event:
            for record in event['Records']:
                key = record['s3']['object']['key']
                load_parquet_to_warehouse(key)
        else:
            # Checks bucket for files if manually triggered
            s3_client = boto3.client("s3")
            response = s3_client.list_objects_v2(Bucket=PROCESSED_BUCKET)
            for obj in response.get('Contents', []):
                load_parquet_to_warehouse(obj['Key'])

        preview_all_tables()
        return {"statusCode": 200, "body": "Load successful"}        
    
    except Exception as e:
        logger.error(f"Load failed: {e}")
        return {"statusCode": 500, "body": str(e)}

