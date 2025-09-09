import boto3
import psycopg2
import pandas as pd
import logging
import json
from io import BytesIO
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

PROCESSED_BUCKET = 'nc-crigglestone-processed-bucket'

WAREHOUSE_TABLES = [
    'dim_counterparty',
    'dim_currency',
    'dim_date',
    'dim_design',
    'dim_location',
    'dim_payment_type',
    'dim_staff',
    'dim_transaction',
    'payment',
    'purchase_order',
    'sales_order'
]

def get_secret() -> dict:
    secret_name = "Project"
    region_name = "eu-west-2"
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region_name)
    try:
        response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        raise e
    secret = response["SecretString"]
    secret_dict = json.loads("{" + secret + "}")
    return secret_dict["warehouse"]

def connect_to_warehouse():
    db_info = get_secret()
    try:
        conn = psycopg2.connect(
            host=db_info["host"],
            port=db_info["port"],
            database=db_info["database"],
            user=db_info["user"],
            password=db_info["password"],
            sslrootcert="SSLCERTIFICATE"
        )
        cur = conn.cursor()
        return conn, cur
    except Exception as e:
        logger.error(f"Warehouse connection failed: {e}")
        raise

def fetch_processed_parquet(s3_client, table_name):
    key = f"processed-{table_name.replace('_', '-')}.parquet"
    logger.info(f"Fetching {key} from {PROCESSED_BUCKET}")
    try:
        data = s3_client.get_object(Bucket=PROCESSED_BUCKET, Key=key)
        return pd.read_parquet(BytesIO(data['Body'].read()))
    except ClientError as e:
        logger.warning(f"No processed file found for {table_name}: {e}")
        return None

def load_to_warehouse(conn, cur, table_name, df):
    if df is None or df.empty:
        logger.info(f"No data for {table_name}, skipping")
        return
    logger.info(f"Loading {table_name} into warehouse")
    cur.execute(f"TRUNCATE TABLE {table_name} CASCADE;")
    cols = ', '.join(df.columns)
    placeholders = ', '.join(['%s'] * len(df.columns))
    insert_query = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})"
    for _, row in df.iterrows():
        cur.execute(insert_query, tuple(row.values))
    conn.commit()
    logger.info(f"Table {table_name} loaded successfully")

def lambda_handler(event, context):
    logger.info("Starting warehousing lambda")
    s3_client = boto3.client("s3")
    conn, cur = connect_to_warehouse()
    for table in WAREHOUSE_TABLES:
        df = fetch_processed_parquet(s3_client, table)
        load_to_warehouse(conn, cur, table, df)
    cur.close()
    conn.close()
    logger.info("Warehousing lambda completed successfully")
