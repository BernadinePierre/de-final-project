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
    data = client.get_object(
        Bucket='nc-crigglestone-ingest-bucket',
        Key=key
    )
    csv_string = StringIO(data['Body'].read().decode('utf-8'))
    return pd.read_csv(csv_string)


def get_keys_for_table(client, table_name):
    logger.info(f'Getting keys')
    files = client.list_objects(
        Bucket='nc-crigglestone-ingest-bucket',
        Prefix=f'{table_name}/'
    )
    if 'Contents' not in files:
        logger.warning(f'Files for table {table_name} not found')
    return [file['Key'] for file in files['Contents']]


def get_from_ingest(client, table_name):
    logger.info(f'Getting data for {table_name} from ingest bucket')
    keys = get_keys_for_table(client, table_name)

    raw_data = fetch_file_from_ingest(client, keys.pop(0))
    for key in keys:
        new_data = fetch_file_from_ingest(client, key)
        raw_data = pd.concat([raw_data, new_data], axis=0)
    return raw_data


def put_in_processed(client, table_name, data):
    logger.info(f'Putting table {table_name} into processed bucket')
    parqueted = data.to_parquet()
    client.put_object(
        Bucket='nc-crigglestone-processed-bucket',
        Key=f'processed-{table_name.replace('_', '-')}.parquet',
        Body=parqueted
    )


def get_from_processed(client, table_name):
    logger.info(f'Fetching data from processed bucket for {table_name}')
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

    logger.info('Getting dates from payment')
    query = "SELECT created_at, last_updated, payment_date FROM payment"
    payment_dates = ps.sqldf(query, {'payment': payments})
    payment_dates = pd.melt(payment_dates)['value']

    query = "SELECT created_at, last_updated, agreed_delivery_date, agreed_payment_date FROM this_table"

    logger.info('Getting dates from sales_order')
    sales_dates = ps.sqldf(query, {'this_table': sales})
    sales_dates = pd.melt(sales_dates)['value']

    logger.info('Getting dates from purchase_order')
    purchase_dates = ps.sqldf(query, {'this_table': purchases})
    purchase_dates = pd.melt(purchase_dates)['value']

    logger.info('Collating dates')
    total_dates = pd.concat([payment_dates, sales_dates, purchase_dates], axis=0)
    total_dates = pd.to_datetime(total_dates, format='mixed')
    total_dates.sort_values(inplace=True)

    logger.info('Creating dates')
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
    dates.insert(0, 'date_id', range(1, len(dates)+1))

    return dates


def make_fact_payment(payment, date):
    payment['created_at'] = pd.to_datetime(payment['created_at'])
    payment['last_updated'] = pd.to_datetime(payment['last_updated'])
    payment['payment_date'] = pd.to_datetime(payment['payment_date']).dt.date

    payment['created_date'] = payment['created_at'].dt.date
    payment['created_time'] = payment['created_at'].dt.time

    payment['last_updated_date'] = payment['last_updated'].dt.date
    payment['last_updated_time'] = payment['last_updated'].dt.time

    date_query = """SELECT
        date_id,
        year || '-' || month || '-' || day AS full_date
    FROM date
    """

    main_query = """SELECT
        payment_id,
        c.date_id AS created_date,
        created_time,
        u.date_id AS last_updated_date,
        last_updated_time,
        transaction_id,
        counterparty_id,
        payment_amount,
        currency_id,
        payment_type_id,
        paid,
        p.date_id AS paymet_date
    FROM payment
    LEFT JOIN dates c ON payment.created_date = c.full_date
    LEFT JOIN dates u ON payment.last_updated_date = u.full_date
    LEFT JOIN dates p ON payment.payment_date = p.full_date
    """

    dates = ps.sqldf(date_query, {'date': date})
    dates['full_date'] = pd.to_datetime(dates['full_date']).dt.date
    processed_payment = ps.sqldf(main_query, {'payment': payment, 'dates': dates})
    processed_payment.insert(0, 'record_payment_id', range(1, len(processed_payment)+1))
    return processed_payment


def make_fact_purchase_order(purchases, date):
    purchases['created_at'] = pd.to_datetime(purchases['created_at'])
    purchases['last_updated'] = pd.to_datetime(purchases['last_updated'])
    purchases['agreed_delivery_date'] = pd.to_datetime(purchases['agreed_delivery_date']).dt.date
    purchases['agreed_payment_date'] = pd.to_datetime(purchases['agreed_payment_date']).dt.date

    purchases['created_date'] = purchases['created_at'].dt.date
    purchases['created_time'] = purchases['created_at'].dt.time

    purchases['last_updated_date'] = purchases['last_updated'].dt.date
    purchases['last_updated_time'] = purchases['last_updated'].dt.time

    date_query = """SELECT
        date_id,
        year || '-' || month || '-' || day AS full_date
    FROM date
    """

    main_query = """SELECT
        purchase_order_id,
        c.date_id AS created_date,
        created_time,
        u.date_id AS last_updated_date,
        last_updated_time,
        staff_id,
        counterparty_id,
        item_code,
        item_quantity,
        item_unit_price,
        currency_id,
        d.date_id AS agreed_delivery_date,
        p.date_id AS agreed_payment_date,
        agreed_delivery_location_id
    FROM purchases
    LEFT JOIN dates c ON purchases.created_date = c.full_date
    LEFT JOIN dates u ON purchases.last_updated_date = u.full_date
    LEFT JOIN dates d ON purchases.agreed_delivery_date = d.full_date
    LEFT JOIN dates p ON purchases.agreed_payment_date = p.full_date
    """

    dates = ps.sqldf(date_query, {'date': date})
    dates['full_date'] = pd.to_datetime(dates['full_date']).dt.date
    processed_purchases = ps.sqldf(main_query, {'purchases': purchases, 'dates': dates})
    processed_purchases.insert(0, 'purchase_record_id', range(1, len(processed_purchases)+1))
    return processed_purchases


def make_fact_sales_order(sales, date):
    sales['created_at'] = pd.to_datetime(sales['created_at'])
    sales['last_updated'] = pd.to_datetime(sales['last_updated'])
    sales['agreed_payment_date'] = pd.to_datetime(sales['agreed_payment_date']).dt.date
    sales['agreed_delivery_date'] = pd.to_datetime(sales['agreed_delivery_date']).dt.date

    sales['created_date'] = sales['created_at'].dt.date
    sales['created_time'] = sales['created_at'].dt.time

    sales['last_updated_date'] = sales['last_updated'].dt.date
    sales['last_updated_time'] = sales['last_updated'].dt.time

    date_query = """SELECT
        date_id,
        year || '-' || month || '-' || day AS full_date
    FROM date
    """

    main_query = """SELECT
        sales_order_id,
        c.date_id AS created_date,
        created_time,
        u.date_id AS last_updated_date,
        last_updated_time,
        staff_id AS sales_staff_id,
        counterparty_id,
        units_sold,
        unit_price,
        currency_id,
        design_id,
        p.date_id AS agreed_payment_date,
        d.date_id AS agreed_delivery_date,
        agreed_delivery_location_id
    FROM sales
    LEFT JOIN dates c ON sales.created_date = c.full_date
    LEFT JOIN dates u ON sales.last_updated_date = u.full_date
    LEFT JOIN dates p ON sales.agreed_payment_date = p.full_date
    LEFT JOIN dates d ON sales.agreed_delivery_date = d.full_date
    """

    dates = ps.sqldf(date_query, {'date': date})
    dates['full_date'] = pd.to_datetime(dates['full_date']).dt.date
    processed_sales = ps.sqldf(main_query, {'sales': sales, 'dates': dates})
    processed_sales.insert(0, 'sales_record_id', range(1, len(processed_sales)+1))
    return processed_sales


# {'updates': {'datetime': '2025-09-04 13:37', 'tables': ['currency', 'payment']}}
def lambda_handler(event, context):
    logger.info('Starting lambda')

    dimensions = {}
    facts = {}
    
    s3_client = boto3.client('s3')

    payment = get_from_ingest(s3_client, 'payment')
    purchase_order = get_from_ingest(s3_client, 'purchase_order')
    sales_order = get_from_ingest(s3_client, 'sales_order')

    dimensions['dim_counterparty'] = make_dim_counterparty(s3_client)
    dimensions['dim_currency'] = make_dim_currency(s3_client)
    dimensions['dim_date'] = make_dim_dates(payment, purchase_order, sales_order)
    dimensions['dim_design'] = make_dim_design(s3_client)
    dimensions['dim_location'] = make_dim_location(s3_client, purchase_order, sales_order)
    dimensions['dim_payment_type'] = make_dim_payment_type(s3_client)
    dimensions['dim_staff'] = make_dim_staff(s3_client)
    dimensions['dim_transaction'] = make_dim_transaction(s3_client)

    facts['payment'] = make_fact_payment(payment, dimensions['dim_date'])
    facts['purchase_order'] = make_fact_purchase_order(purchase_order, dimensions['dim_date'])
    facts['sales_order'] = make_fact_sales_order(sales_order, dimensions['dim_date'])

    for table in dimensions.keys():
        put_in_processed(s3_client, table, dimensions[table])

    for table in facts.keys():
        put_in_processed(s3_client, table, facts[table])


if __name__ == '__main__':
    logging.getLogger().addHandler(logging.StreamHandler())
    lambda_handler({}, {})