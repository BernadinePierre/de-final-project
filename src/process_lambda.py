import boto3
from botocore.exceptions import ClientError
import pandas as pd
import pandasql as ps
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
    if 'Contents' not in files:
        logger.warning(f'Files for table {table_name} not found')
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
    run_query = lambda query, tables: ps.sqldf(query, tables)

    payment = get_from_ingest(s3_client, 'payment')
    purchase_order = get_from_ingest(s3_client, 'purchase_order')
    sales_order = get_from_ingest(s3_client, 'sales_order')

    logger.info('Creating dim_location')
    # DIM_LOCATION
    address = get_from_ingest(s3_client, 'address')
    address.drop_duplicates(subset=['address_id'], keep='last', inplace=True)
    query = """SELECT
        address_id AS location_id,
        address_line_1,
        address_line_2,
        district,
        city,
        postal_code,
        country,
        phone
    FROM address
    """
    dimensions['dim_location'] = run_query(query, {'address': address, 'purchase_order': purchase_order, 'sales_order': sales_order})
    
    logger.info('Creating dim_counterparty')
    # DIM_COUNTERPARTY
    counterparty = get_from_ingest(s3_client, 'counterparty')
    counterparty.drop_duplicates(subset=['counterparty_id'], keep='last', inplace=True)
    query = """SELECT
        counterparty_id,
        counterparty_legal_name,
        address_line_1 AS counterparty_legal_address_line_1,
        address_line_2 AS counterparty_legal_address_line_2,
        district AS counterparty_legal_district,
        city AS counterparty_legal_city,
        postal_code AS counterparty_legal_postal_code,
        country AS counterparty_legal_country,
        phone AS counterparty_legal_phone_number
    FROM counterparty
    JOIN address ON counterparty.legal_address_id = address.address_id
    """
    dimensions['dim_counterparty'] = run_query(query, {'counterparty': counterparty, 'address': address})
    
    # DIM_CURRENCY
    # TODO Add currency_name
    currency = get_from_ingest(s3_client, 'currency')
    currency.drop_duplicates(subset=['currency_id'], keep='last', inplace=True)
    query = "SELECT currency_id, currency_code FROM currency"
    dimensions['dim_currency'] = run_query(query, {'currency': currency})
    
    logger.info('Creating dim_design')
    # DIM_DESIGN
    design = get_from_ingest(s3_client, 'design')
    design.drop_duplicates(subset=['design_id'], keep='last', inplace=True)
    query = "SELECT design_id, design_name, file_location, file_name FROM design"
    dimensions['dim_design'] = run_query(query, {'design': design})
    
    logger.info('Creating dim_paymet_design')
    # DIM_PAYMENT_TYPE
    payment_type = get_from_ingest(s3_client, 'payment_type')
    payment_type.drop_duplicates(subset=['payment_type_id'], keep='last', inplace=True)
    query = "SELECT payment_type_id, payment_type_name FROM payment_type"
    dimensions['dim_payment_type'] = run_query(query, {'payment_type': payment_type})
    
    # DIM_STAFF
    department = get_from_ingest(s3_client, 'department')
    department.drop_duplicates(subset=['department_id'], keep='last', inplace=True)
    staff = get_from_ingest(s3_client, 'staff')
    staff.drop_duplicates(subset=['staff_id'], keep='last', inplace=True)
    query = """SELECT
        staff_id,
        first_name,
        last_name,
        department_name,
        location,
        email_address
    FROM staff
    JOIN department ON staff.department_id = department.department_id
    """
    dimensions['dim_staff'] = run_query(query, {'staff': staff, 'department': department})
    
    logger.info('Creating dim_transaction')
    # DIM_TRANSACTION
    transact = get_from_ingest(s3_client, 'transaction')
    transact.drop_duplicates(subset=['transaction_id'], keep='last', inplace=True)
    query = "SELECT transaction_id, transaction_type, sales_order_id, purchase_order_id FROM transact"
    dimensions['dim_transaction'] = run_query(query, {'transact': transact})

    for table in dimensions.keys():
        logger.info(f'>>> {table}')
        logger.info(dimensions[table])


if __name__ == '__main__':
    logging.getLogger().addHandler(logging.StreamHandler())
    lambda_handler({}, {})