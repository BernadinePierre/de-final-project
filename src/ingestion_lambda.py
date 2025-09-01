import boto3
from botocore.exceptions import ClientError
import json
import psycopg2
import pandas as pd
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def get_secret() -> dict:
    secret_name = "Project"
    region_name = "eu-west-2"
    # Create a Secrets Manager client
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
    secret = get_secret_value_response['SecretString']

    secret_dict = json.loads('{'+secret+'}')
    return secret_dict['crigglestone']


def connect_to_original_database():
    database_info = get_secret()
    
    try:
        conn = psycopg2.connect(
            host=database_info['host'],
            port=database_info['port'],
            database=database_info['database'],
            user=database_info['user'],
            password=database_info['password'],
            sslrootcert='SSLCERTIFICATE'
        )
        cur = conn.cursor()
        return cur
    except Exception as e:
        logger.warning(f'Database connection failed due to {e}')


def check_original_update(table_name, connection):
    logger.info(f'Getting last update')
    query = f'SELECT last_updated FROM {table_name} ORDER BY last_updated DESC LIMIT 1'
    connection.execute(query)
    last_update = connection.fetchall()[0][0].strftime(r'%Y-%m-%d')
    logger.info(f'Table {table_name} last updated at {last_update}')
    return last_update


def get_original_updates(table_name, connection):
    logger.info(f'Getting updated data')
    query = f'SELECT COUNT(last_updated) FROM {table_name}'
    connection.execute(query)
    return [col[0] for col in connection.description], connection.fetchall()


def put_in_s3(table, data, date, client):
    logger.info('Putting data into S3')
    client.put_object(
        Bucket='nc-crigglestone-ingest-bucket',
        Key=f'{table}/{date}.csv',
        Body=data.to_csv(index=False)
    )
    logger.info(f'Table {table} updated into S3')


def lambda_handler(event, context):
    logger.info("Lambda ingestion job started")

    connection = connect_to_original_database()
    s3_client = boto3.client('s3')

    table_list = [
        'address',
        'counterparty',
        'currency',
        'department',
        'design',
        'payment',
        'payment_type',
        'purchase_order',
        'sales_order',
        'staff',
        'transaction'
    ]

    for table in table_list:
        logger.info(f'Starting table {table}')
        date = check_original_update(table, connection)

        columns, data = get_original_updates(table, connection)
        data = pd.DataFrame(data, columns=columns)

        put_in_s3(table, data, date, s3_client)