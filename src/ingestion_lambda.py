import boto3
from botocore.exceptions import ClientError
import json
import psycopg2
import pandas as pd
import logging

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


def check_original_update(table_name):
    connection = connect_to_original_database()
    query = f'SELECT last_updated FROM {table_name} ORDER BY last_updated DESC LIMIT 1'
    connection.execute(query)
    return connection.fetchall()[0][0].strftime(r'%Y-%m-%d')
    # print(f'Table <{table}> was last updated at:\n{result[0][0].strftime(r'%Y-%m-%d')}')


def get_original_updates(table_name):
    connection = connect_to_original_database()
    query = f'SELECT * FROM {table_name} LIMIT 5'
    connection.execute(query)
    return [col[0] for col in connection.description], connection.fetchall()


def connect_to_s3():
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
    s3_client = boto3.client('s3')
    for table in table_list:
        date = check_original_update(table)
        # TODO
        # check here whether we need to update or can move on
        # or create a minimised list of tables to loop over before

        columns, data = get_original_updates(table)
        data = pd.DataFrame(data, columns=columns)
        s3_client.put_object(
            Bucket='nc-crigglestone-ingest-bucket',
            Key=f'{table}/{date}.csv',
            Body=data.to_csv(index=False)
        )
        print(f'Table {table} updated into S3')

def ingestion_lambda_handler(event, context):
    logging.info("Lambda ingestion job started")
    try:
        connect_to_s3()
        return {
            "statusCode": 200,
            "body": json.dumps("Ingestion completed successfully")
        }
    except Exception as e:
        logging.error(f"Lambda ingestion failed: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps(f"Error: {str(e)}")
        }