import boto3
from botocore.exceptions import ClientError
import json
import psycopg2
import psycopg2.extras
import pandas as pd
import logging
import os

logger = logging.getLogger()
logger.setLevel(logging.INFO)

PROCESSED_BUCKET = "nc-crigglestone-processed-bucket"

def get_rds_secret() -> dict:

    secret_name = "warehouse-db-credentials"
    region_name = "eu-west-2"

    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        raise e
    
    secret = json.loads(get_secret_value_response['SecretString'])

    return secret

def connect_to_warehouse():
    database_info = get_rds_secret()
    
    try:
        conn = psycopg2.connect(
            host=database_info['host'],
            port=database_info['port'],
            database=database_info['database'],
            user=database_info['user'],
            password=database_info['password']
        )
        logger.info("Successfully connected to RDS warehouse.")
        return conn
    except Exception as e:
        logger.warning(f'Database connection failed due to {e}')
        raise

def load_parquet_to_warehouse(key):
    table = os.path.basename(key).replace("processed-dim", "").replace(".parquet","")

    s3_client = boto3.client("s3")
    obj = s3_client.get_object(Bucket=PROCESSED_BUCKET, Key=key)
    df = pd.read_parquet(obj['Body'])
    
    conn = connect_to_warehouse()
    cur = conn.cursor()

    # Convert DataFrame to list of tuples
    records = [tuple(x) for x in df.to_numpy()]

    # Build placeholder SQL
    columns = ','.join(df.columns)
    insert_query = f"INSERT INTO {table} ({columns}) VALUES %s"

    try:
        psycopg2.extras.execute_values(
            cur, insert_query, records, page_size=100
        )
        conn.commit()
        logger.info(f"Loaded {len(df)} rows into {table}")
    except Exception as e:
        logger.error(f"Insert failed: {e}")
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

def lambda_handler(event, context):
    logger.info("Warehouse loader started")
    try:
        if 'Records' in event:
            for record in event['Records']:
                key = record['s3']['object']['key']
                load_parquet_to_warehouse(key)
        else:
            # If manually triggered, optionally scan bucket for files
            s3_client = boto3.client("s3")
            response = s3_client.list_objects_v2(Bucket=PROCESSED_BUCKET)
            for obj in response.get('Contents', []):
                load_parquet_to_warehouse(obj['Key'])

        return {"statusCode": 200, "body": "Load successful"}
    
    except Exception as e:
        logger.error(f"Load failed: {e}")
        return {"statusCode": 500, "body": str(e)}     
