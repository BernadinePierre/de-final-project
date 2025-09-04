import boto3
from botocore.exceptions import ClientError
import json
import psycopg2
import pandas as pd
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

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
        cur = conn.cursor()
        return cur
    except Exception as e:
        logger.warning(f'Database connection failed due to {e}')
        raise

def load_parquet_to_warehouse(key, table):
    pass

def lambda_handler(event, context):
    logger.info("WAREHOUSE LAMBDA STARTED")
    conn = connect_to_warehouse()
    logger.info("Connected to RDS successfully")
    return {"statusCode": 200, "body": "Done"}