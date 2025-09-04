import boto3
from botocore.exceptions import ClientError
import pandas as pd
import pandasql as ps
import logging
from io import StringIO, BytesIO


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


def put_in_processed(client, table_name, data):
    parqueted = data.to_parquet()
    client.put_object(
        Bucket='nc-crigglestone-processed-bucket',
        Key=f'processed-{table_name.replace('_', '-')}.parquet',
        Body=parqueted
    )


def get_from_processed(client, table_name):
    BUCKET = 'nc-crigglestone-processed-bucket'
    KEY = f'processed-{table_name.replace('_', '-')}.parquet'
    try:
        client.head_object(
            Bucket=BUCKET,
            Key=KEY
        )
        data = client.get_object(
            Bucket=BUCKET,
            Key=KEY
        )
        return pd.read_parquet(BytesIO(data['Body'].read()))
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            # logger.info('Processed file does not exist')
            return None
        else:
            raise


# def get_dim_location(client, purchase, sale):
#     logger.info('Creating dim_location')
#     preexisting = get_from_processed(client, 'dim_location')
#     if preexisting is None:
#         preexisting = pd.DataFrame({
#             'location_id': [],
#             'address_line_1': [],
#             'address_line_2': [],
#             'district': [],
#             'city': [],
#             'postal_code': [],
#             'country': [],
#             'phone': []
#         })
#     address = get_from_ingest(client, 'address')
#     address.drop_duplicates(subset=['address_id'], keep='last', inplace=True)
#     query = """SELECT
#         address_id AS location_id,
#         address_line_1,
#         address_line_2,
#         district,
#         city,
#         postal_code,
#         country,
#         phone
#     FROM address
#     """
#     new_locations = ps.psqldf(query, {'address': address, 'purchase_order': purchase, 'sales_order': sale})
#     new_locations.insert(0, 'location_id', range(len(preexisting), len(preexisting)+len(new_locations)))
#     return pd.concat([preexisting, new_locations], axis=0)


def make_dim_location(client, purchase, sale):
    logger.info('Creating dim_location')
    address = get_from_ingest(client, 'address')
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
    return ps.sqldf(query, {'address': address, 'purchase_order': purchase, 'sales_order': sale})


def make_dim_counterparty(client):
    logger.info('Creating dim_counterparty')
    counterparty = get_from_ingest(client, 'counterparty')
    counterparty.drop_duplicates(subset=['counterparty_id'], keep='last', inplace=True)
    address = get_from_ingest(client, 'address')
    address.drop_duplicates(subset=['address_id'], keep='last', inplace=True)
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
    return ps.sqldf(query, {'counterparty': counterparty, 'address': address})


def make_dim_currency(client):
    # TODO Add currency_name
    logger.info('Creating dim_currency')
    currency = get_from_ingest(client, 'currency')
    currency.drop_duplicates(subset=['currency_id'], keep='last', inplace=True)
    query = "SELECT currency_id, currency_code FROM currency"
    return ps.sqldf(query, {'currency': currency})


def make_dim_design(client):
    logger.info('Creating dim_design')
    design = get_from_ingest(client, 'design')
    design.drop_duplicates(subset=['design_id'], keep='last', inplace=True)
    query = "SELECT design_id, design_name, file_location, file_name FROM design"
    return ps.sqldf(query, {'design': design})


def make_dim_payment_type(client):
    logger.info('Creating dim_paymet_design')
    payment_type = get_from_ingest(client, 'payment_type')
    payment_type.drop_duplicates(subset=['payment_type_id'], keep='last', inplace=True)
    query = "SELECT payment_type_id, payment_type_name FROM payment_type"
    return ps.sqldf(query, {'payment_type': payment_type})


def make_dim_staff(client):
    logger.info('Creating dim_staff')
    staff = get_from_ingest(client, 'staff')
    staff.drop_duplicates(subset=['staff_id'], keep='last', inplace=True)
    department = get_from_ingest(client, 'department')
    department.drop_duplicates(subset=['department_id'], keep='last', inplace=True)
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
    return ps.sqldf(query, {'staff': staff, 'department': department})


def make_dim_transaction(client):
    logger.info('Creating dim_transaction')
    transact = get_from_ingest(client, 'transaction')
    transact.drop_duplicates(subset=['transaction_id'], keep='last', inplace=True)
    query = "SELECT transaction_id, transaction_type, sales_order_id, purchase_order_id FROM transact"
    return ps.sqldf(query, {'transact': transact})


def make_dim_dates(payments, purchases, sales):
    logger.info('Creating dim_date')
    query = "SELECT created_at, last_updated, payment_date FROM payment"
    payment_dates = ps.sqldf(query, {'payment': payments})
    payment_dates = pd.melt(payment_dates)['value']
    query = "SELECT created_at, last_updated, agreed_delivery_date, agreed_payment_date FROM this_table"
    sales_dates = ps.sqldf(query, {'this_table': sales})
    sales_dates = pd.melt(sales_dates)['value']
    purchase_dates = ps.sqldf(query, {'this_table': purchases})
    purchase_dates = pd.melt(purchase_dates)['value']
    total_dates = pd.concat([payment_dates, sales_dates, purchase_dates], axis=0)
    total_dates = pd.to_datetime(total_dates, format='mixed')
    total_dates.sort_values(inplace=True)
    dates = pd.DataFrame({
        'year': total_dates.dt.year,
        'month': total_dates.dt.month,
        'day': total_dates.dt.day,
        'day_of_week': total_dates.dt.day_of_week,
        'day_name': total_dates.dt.day_name(),
        'month_name': total_dates.dt.month_name(),
        'quarter': total_dates.dt.quarter
    })
    dates.drop_duplicates(keep='first', inplace=True)
    dates.insert(0, 'date_id', range(0, len(dates)))
    return dates


# {'updates': {'datetime': '2025-09-04 13:37', 'tables': ['currency', 'payment']}}
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

    payment = get_from_ingest(s3_client, 'payment')
    purchase_order = get_from_ingest(s3_client, 'purchase_order')
    sales_order = get_from_ingest(s3_client, 'sales_order')

    dimensions['dim_location'] = make_dim_location(s3_client, purchase_order, sales_order)
    dimensions['dim_counterparty'] = make_dim_counterparty(s3_client)
    dimensions['dim_currency'] = make_dim_currency(s3_client)
    dimensions['dim_design'] = make_dim_design(s3_client)
    dimensions['dim_payment_type'] = make_dim_payment_type(s3_client)
    dimensions['dim_staff'] = make_dim_staff(s3_client)
    dimensions['dim_transaction'] = make_dim_transaction(s3_client)
    dimensions['dim_date'] = make_dim_dates(payment, purchase_order, sales_order)

    for table in dimensions.keys():
        put_in_processed(s3_client, table, dimensions[table])


if __name__ == '__main__':
    logging.getLogger().addHandler(logging.StreamHandler())
    lambda_handler({}, {})