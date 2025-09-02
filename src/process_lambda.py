import boto3
from botocore.exceptions import ClientError
import pandas as pd
import logging
from io import StringIO


logger = logging.getLogger()
logger.setLevel(logging.INFO)


STARTING_TABLES = [
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


def fetch_file_from_ingest(client, key):
    return pd.read_csv(StringIO(client.get_object(
        Bucket='nc-crigglestone-ingest-bucket',
        Key=key
    )['Body'].read().decode('utf-8')))


def get_keys_for_table(client, table_name):
    files = client.list_objects(
        Bucket='nc-crigglestone-ingest-bucket',
        Prefix=f'{table_name}/'
    )
    return [file['Key'] for file in files['Contents']]


def get_from_ingest(client, table_name, reduce=True):
    keys = get_keys_for_table(client, table_name)

    raw_data = fetch_file_from_ingest(client, keys.pop(0))
    for key in keys:
        new_data = fetch_file_from_ingest(client, key)
        raw_data = pd.concat([raw_data, new_data], axis=0)
    return raw_data


def put_in_processed():
    raise NotImplementedError


def lambda_handler(event, context):
    logger.info('Starting lambda')

    dimensions = {
        'dim_counterparty': pd.DataFrame({
            'counterparty_id': [],
            'counterparty_legal_name': [],
            'counterparty_legal_address_line_1': [],
            'counterparty_legal_address_line_2': [],
            'counterparty_legal_district': [],
            'counterparty_legal_city': [],
            'counterparty_legal_postal_code': [],
            'counterparty_legal_country': [],
            'counterparty_legal_phone_number': []
        }),
        'dim_currency': pd.DataFrame({
            'currency_id': [],
            'currency_code': [],
            'currency_name': []
        }),
        'dim_date': pd.DataFrame({
            'date_id': [],
            'year': [],
            'month': [],
            'day': [],
            'day_of_week': [],
            'day_name': [],
            'month_name': [],
            'quarter': []
        }),
        'dim_design': pd.DataFrame({
            'design_id': [],
            'design_name': [],
            'file_location': [],
            'file_name': []
        }),
        'dim_location': pd.DataFrame({
            'location_id': [],
            'address_line_1': [],
            'address_line_2': [],
            'district': [],
            'city': [],
            'postal_code': [],
            'country': [],
            'phone': []
        }),
        'dim_payment_type': pd.DataFrame({
            'payment_type_id': [],
            'payment_type_name': []
        }),
        'dim_staff': pd.DataFrame({
            'staff_id': [],
            'first_name': [],
            'last_name': [],
            'department_name': [],
            'location': [],
            'email_address': []
        }),
        'dim_transaction': pd.DataFrame({
            'transaction_id': [],
            'transaction_type': [],
            'sales_order_id': [],
            'purchase_order_id': []
        })
    }
    
    s3_client = boto3.client('s3')
    
    for table in STARTING_TABLES:
        logger.info(f'>>> Table {table}')
        logger.info(get_from_ingest(s3_client, table))


if __name__ == '__main__':
    logging.getLogger().addHandler(logging.StreamHandler())
    lambda_handler({}, {})