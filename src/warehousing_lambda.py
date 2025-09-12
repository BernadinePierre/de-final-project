import awswrangler as wr
import  pg8000
import boto3
from botocore.exceptions import ClientError
import json
import logging


logger = logging.getLogger()
logger.setLevel(logging.INFO)
logging.basicConfig(format='[%(levelname)s] %(message)s')


PROCESSED_BUCKET = "nc-crigglestone-processed-bucket"


def get_rds_secret() -> dict:
    logger.info('Collecting secret')

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

        logger.info('Successfully collected secret')
        return secret
    except ClientError as e:
        raise e 


def connect_to_warehouse():
    logger.info('Connecting to RDS warehouse')

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


def load_parquet_to_warehouse(key, connection):
    table_name = key.replace("dim-", "").replace("fact-","").replace(".parquet", "")
    table_name = table_name.replace("-", "_")

    logger.info(f"Loading file {key} into table {table_name}")
    
    logger.info(f'[{table_name}] Collecting data from processed bucket')
    s3_path = f"s3://{PROCESSED_BUCKET}/{key}"
    processed_data = wr.s3.read_parquet(s3_path)
    if processed_data.empty:
        logger.error(f"No data found in {table_name}")
        return
    else:
        logger.info(f'[{table_name}] Table collected')
    
    try:
        logger.info(f'[{table_name}] Putting data into database')
        # Load into Postgres in batches
        wr.postgresql.to_sql(
            df=processed_data,
            table=table_name,
            con=connection,
            schema="public",        
            mode="overwrite",     
            chunksize=500       
        )
        logger.info(f"Loaded {len(processed_data)} rows into {table_name}")
    
    except Exception as e:
        logger.error(f"Failed to load {table_name} into {table_name}: {e}")
        raise


def preview_all_tables(connection):
    """
    Logs the first 10 rows of every table in the 'public' schema of the warehouse.
    """
    
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
            
            # Fetch first 10 rows
            df_preview = wr.postgresql.read_sql_query(
                sql=f'SELECT * FROM "{table}" LIMIT 10;',
                con=connection
            )
            if df_preview.empty:
                logger.info(f"No data in table {table}")
            else:
                logger.info(f"\n{df_preview.to_string(index=False)}")
            
            logger.info("-" * 50)
        
        for table in table_names:
            df = wr.postgresql.read_sql_query(sql=f'SELECT * FROM "{table}"', con=connection)
            s3_path = f"s3://nc-crigglestone-lambda-bucket/extracts/{table}.csv"
            wr.s3.to_csv(df, path=s3_path, index=False)

    except Exception as e:
        logger.error(f"Failed to preview tables: {e}")
        raise


def lambda_handler(event, context):
    logger.info("Warehouse loader started")

    connection = connect_to_warehouse()

    try:
        if 'Records' in event:
            logger.info(f'Updates recieved from processing lambda: {', '.join(event['Records'])}')
            for key in event['Records']:
                load_parquet_to_warehouse(key, connection)
        else:
            # If manually triggered, optionally scan bucket for files
            logger.info('No records found, loading in all files')
            s3_client = boto3.client("s3")
            response = s3_client.list_objects_v2(Bucket=PROCESSED_BUCKET)
            for obj in response.get('Contents', []):
                load_parquet_to_warehouse(obj['Key'], connection)

        # preview_all_tables(connection)

        logger.info('Warehouse loader completed')
        connection.close()
        return {"statusCode": 200, "body": "Load successful"}
    
    except Exception as e:
        logger.error(f"Load failed: {e}")
        connection.close()
        return {"statusCode": 500, "body": str(e)}