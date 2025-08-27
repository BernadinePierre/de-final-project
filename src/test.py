import boto3
from botocore.exceptions import ClientError
import json
import psycopg2
import pandas as pd


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
        print(f'Database connection failed due to {e}')


def check_original_updates():
    connection = connect_to_original_database()
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
        query = f'SELECT last_updated FROM {table} ORDER BY last_updated DESC LIMIT 1'
        connection.execute(query)
        result = connection.fetchall()
        print(f'Table <{table}> was last updated at:\n{result[0][0].strftime(r'%Y-%m-%d')}')


def lambda_handler(event, context):
    pass


if __name__ == '__main__':
    check_original_updates()