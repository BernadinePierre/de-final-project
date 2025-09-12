import boto3
from botocore.exceptions import ClientError
import pandas as pd
import logging
from io import StringIO, BytesIO
import json


logger = logging.getLogger()
logger.setLevel(logging.INFO)
logging.basicConfig(format='[%(levelname)s] %(message)s')


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
    files = client.list_objects(
        Bucket='nc-crigglestone-ingest-bucket',
        Prefix=f'{table_name}/'
    )
    if 'Contents' not in files:
        logger.warning(f'Files for table {table_name} not found')
    return [file['Key'] for file in files['Contents']]


def get_from_ingest(client, table_name, preamble):
    logger.info(f'{preamble} Getting keys for {table_name} from ingest bucket')
    keys = get_keys_for_table(client, table_name)

    logger.info(f'{preamble} Fetching data')
    raw_data = fetch_file_from_ingest(client, keys.pop(0))
    for key in keys:
        new_data = fetch_file_from_ingest(client, key)
        raw_data = pd.concat([raw_data, new_data], axis=0)
    logger.info(f'{preamble} Data collected')
    return raw_data


def put_in_processed(client, table_name, data):
    logger.info(f'[{table_name}] Converting to parquet')
    parqueted = data.to_parquet(index=False)
    logger.info(f'[{table_name}] Uploading to bucket')
    client.put_object(
        Bucket='nc-crigglestone-processed-bucket',
        Key=f'{table_name.replace('_', '-')}.parquet',
        Body=parqueted
    )
    logger.info(f'[{table_name}] File uploaded')


def get_parquet(client, table_name):
    logger.info(f'Fetching data from processed bucket for {table_name}')
    BUCKET = 'nc-crigglestone-processed-bucket'
    KEY = f'{table_name.replace('_', '-')}.parquet'
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
            logger.error('Processed file does not exist')
            return None
        else:
            raise


def make_dim_location(client):
    logger.info('Creating dim_location')

    address = get_from_ingest(client, 'address', '[dim_location]')
    address.drop_duplicates(subset=['address_id'], keep='last', inplace=True)

    address.rename(columns={'address_id': 'location_id'}, inplace=True)

    new_data = address[[
        'location_id',
        'address_line_1',
        'address_line_2',
        'district',
        'city',
        'postal_code',
        'country',
        'phone'
    ]]

    logger.info('[dim_location] Table created')
    return new_data


def make_dim_counterparty(client):
    logger.info('Creating dim_counterparty')

    counterparty = get_from_ingest(client, 'counterparty', '[dim_counterparty]')
    counterparty.drop_duplicates(subset=['counterparty_id'], keep='last', inplace=True)

    address = get_from_ingest(client, 'address', '[dim_counterparty]')
    address.drop_duplicates(subset=['address_id'], keep='last', inplace=True)
    address.rename(columns={'address_id': 'legal_address_id'}, inplace=True)

    logger.info('[dim_counterparty] Joining address to counterparty')
    dim_counterparty = counterparty.join(address, on='legal_address_id', rsuffix='_address')
    logger.info('[dim_counterparty] Tables joined')

    dim_counterparty.rename(
        columns={
            'address_line_1': 'counterparty_legal_address_line_1',
            'address_line_2': 'counterparty_legal_address_line_2',
            'district': 'counterparty_legal_district',
            'city': 'counterparty_legal_city',
            'postal_code': 'counterparty_legal_postal_code',
            'country': 'counterparty_legal_country',
            'phone': 'counterparty_legal_phone_number'
        },
        inplace=True
    )
    new_data = dim_counterparty[[
        'counterparty_id',
        'counterparty_legal_name',
        'counterparty_legal_address_line_1',
        'counterparty_legal_address_line_2',
        'counterparty_legal_district',
        'counterparty_legal_city',
        'counterparty_legal_postal_code',
        'counterparty_legal_country',
        'counterparty_legal_phone_number'
    ]]

    logger.info('[dim_counterparty] Table created')
    return new_data


def make_dim_currency(client):
    # TODO Add currency_name
    logger.info('Creating dim_currency')

    currency = get_from_ingest(client, 'currency', '[dim_currency]')
    currency.drop_duplicates(subset=['currency_id'], keep='last', inplace=True)

    new_data = currency[['currency_id', 'currency_code']]
    logger.info('[dim_currency] Table created')
    return new_data


def make_dim_design(client):
    logger.info('Creating dim_design')

    design = get_from_ingest(client, 'design', '[dim_design]')
    design.drop_duplicates(subset=['design_id'], keep='last', inplace=True)

    new_data = design[['design_id', 'design_name', 'file_location', 'file_name']]
    logger.info('[dim_design] Table created')
    return new_data


def make_dim_payment_type(client):
    logger.info('Creating dim_paymet_design')

    payment_type = get_from_ingest(client, 'payment_type', '[dim_payment_type]')
    payment_type.drop_duplicates(subset=['payment_type_id'], keep='last', inplace=True)

    new_data = payment_type[['payment_type_id', 'payment_type_name']]
    logger.info('[dim_payment_type] Table created')
    return new_data


def make_dim_staff(client):
    logger.info('Creating dim_staff')

    staff = get_from_ingest(client, 'staff', '[dim_staff]')
    staff.drop_duplicates(subset=['staff_id'], keep='last', inplace=True)

    department = get_from_ingest(client, 'department', '[dim_staff]')
    department.drop_duplicates(subset=['department_id'], keep='last', inplace=True)

    logger.info('[dim_staff] Joining department to staff')
    dim_staff = staff.join(department, on='department_id', rsuffix='department')
    logger.info('[dim_staff] Tables joined')

    new_data = dim_staff[[
        'staff_id',
        'first_name',
        'last_name',
        'department_name',
        'location',
        'email_address'
    ]]
    logger.info('[dim_staff] Table created')
    return new_data


def make_dim_transaction(client):
    logger.info('Creating dim_transaction')

    transact = get_from_ingest(client, 'transaction', '[dim_transaction]')
    transact.drop_duplicates(subset=['transaction_id'], keep='last', inplace=True)

    new_data = transact[[
        'transaction_id',
        'transaction_type',
        'sales_order_id',
        'purchase_order_id'
    ]]
    logger.info('[dim_transaction] Table created')
    return new_data


def make_dim_dates(payments, purchases, sales):
    logger.info('Creating dim_date')

    logger.info('[dim_date] Getting dates from payment')
    payment_dates = payments[['created_at', 'last_updated', 'payment_date']]
    payment_dates = pd.melt(payment_dates)['value']

    logger.info('[dim_date] Getting dates from sales_order')
    sales_dates = purchases[['created_at', 'last_updated', 'agreed_delivery_date', 'agreed_payment_date']]
    sales_dates = pd.melt(sales_dates)['value']

    logger.info('[dim_date] Getting dates from purchase_order')
    purchase_dates = sales[['created_at', 'last_updated', 'agreed_delivery_date', 'agreed_payment_date']]
    purchase_dates = pd.melt(purchase_dates)['value']

    logger.info('[dim_date] Collating dates')
    total_dates = pd.concat([payment_dates, sales_dates, purchase_dates], axis=0)
    total_dates = pd.to_datetime(total_dates, format='mixed')
    total_dates.sort_values(inplace=True)

    logger.info('[dim_date] Creating dates')
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

    logger.info('[dim_date] Table created')
    return dates


def make_fact_payment(payment: pd.DataFrame, dates: pd.DataFrame):
    logger.info('Creating fact_payment')

    logger.info('[fact_payment] Converting to datetimes')
    payment['created_at'] = pd.to_datetime(payment['created_at'])
    payment['last_updated'] = pd.to_datetime(payment['last_updated'])
    payment['payment_date'] = pd.to_datetime(payment['payment_date']).dt.date

    logger.info('[fact_payment] Separating date and time')

    payment['created_date'] = payment['created_at'].dt.date
    payment['created_time'] = payment['created_at'].dt.strftime('%H:%M:%S.%f')

    payment['last_updated_date'] = payment['last_updated'].dt.date
    payment['last_updated_time'] = payment['last_updated'].dt.strftime('%H:%M:%S.%f')

    logger.info('[fact_payment] Joining dates on created_at')
    created = payment.join(
        dates.rename(columns={'date_id': 'c_date_id', 'date': 'created_date'}),
        on='created_date',
        rsuffix='_2'
    )
    logger.info('[fact_payment] Joining dates on last_updated')
    updated = created.join(
        dates.rename(columns={'date_id': 'u_date_id', 'date': 'last_updated_date'}),
        on='last_updated_date',
        rsuffix='_2'
    )
    logger.info('[fact_payment] Joining dates on payment_date')
    paymented = updated.join(
        dates.rename(columns={'date_id': 'p_date_id', 'date': 'payment_date'}),
        on='payment_date',
        rsuffix='_2'
    )
    logger.info('[fact_payment] All dates joined')

    logger.info('[fact_payment] Selecting columns and renaming')
    processed_payment = paymented[[
        'payment_id',
        'c_date_id',
        'created_time',
        'u_date_id',
        'last_updated_time',
        'transaction_id',
        'counterparty_id',
        'payment_amount',
        'currency_id',
        'payment_type_id',
        'paid',
        'p_date_id'
    ]]
    processed_payment.rename(
        columns={
            'c_date_id': 'created_date',
            'u_date_id': 'last_updated_date',
            'p_date_id': 'payment_date'
        },
        inplace=True
    )
    processed_payment.insert(0, 'record_payment_id', range(1, len(processed_payment)+1))
    
    logger.info('[fact_payment] Table created')
    return processed_payment


def make_fact_purchase_order(purchases: pd.DataFrame, dates: pd.DataFrame):
    logger.info('Creating fact_purchase_order')

    logger.info('[fact_purchase_order] Converting to datetimes')
    purchases['created_at'] = pd.to_datetime(purchases['created_at'])
    purchases['last_updated'] = pd.to_datetime(purchases['last_updated'])
    purchases['agreed_delivery_date'] = pd.to_datetime(purchases['agreed_delivery_date']).dt.date
    purchases['agreed_payment_date'] = pd.to_datetime(purchases['agreed_payment_date']).dt.date

    logger.info('[fact_purchase_order] Separating date and time')

    purchases['created_date'] = purchases['created_at'].dt.date
    purchases['created_time'] = purchases['created_at'].dt.strftime('%H:%M:%S.%f')

    purchases['last_updated_date'] = purchases['last_updated'].dt.date
    purchases['last_updated_time'] = purchases['last_updated'].dt.strftime('%H:%M:%S.%f')

    logger.info('[fact_purchase_order] Joining dates on created_at')
    created = purchases.join(
        dates.rename(columns={'date_id': 'c_date_id', 'date': 'created_date'}),
        on='created_date',
        rsuffix='_2'
    )
    logger.info('[fact_purchase_order] Joining dates on last_updated')
    updated = created.join(
        dates.rename(columns={'date_id': 'u_date_id', 'date': 'last_updated_date'}),
        on='last_updated_date',
        rsuffix='_2'
    )
    logger.info('[fact_purchase_order] Joining dates on agreed_delivery_date')
    delivered = updated.join(
        dates.rename(columns={'date_id': 'd_date_id', 'date': 'agreed_delivery_date'}),
        on='agreed_delivery_date',
        rsuffix='_2'
    )
    logger.info('[fact_purchase_order] Joining dates on payment_date')
    paymented = delivered.join(
        dates.rename(columns={'date_id': 'p_date_id', 'date': 'payment_date'}),
        on='agreed_payment_date',
        rsuffix='_2'
    )
    logger.info('[fact_purchase_order] All dates joined')

    logger.info('[fact_purchase_order] Selecting columns and renaming')
    processed_purchases = paymented[[
        'purchase_order_id',
        'c_date_id',
        'created_time',
        'u_date_id',
        'last_updated_time',
        'staff_id',
        'counterparty_id',
        'item_code',
        'item_quantity',
        'item_unit_price',
        'currency_id',
        'd_date_id',
        'p_date_id',
        'agreed_delivery_location_id'
    ]]
    processed_purchases.rename(
        columns={
            'c_date_id': 'created_date',
            'u_date_id': 'last_updated_date',
            'd_date_id': 'agreed_delivery_date',
            'p_date_id': 'agreed_payment_date'
        },
        inplace=True
    )
    processed_purchases.insert(0, 'purchase_record_id', range(1, len(processed_purchases)+1))

    logger.info('[fact_purchase_order] Table created')
    return processed_purchases


def make_fact_sales_order(sales: pd.DataFrame, dates: pd.DataFrame):
    logger.info('Creating fact_sales_order')

    logger.info('[fact_sales_order] Converting to datetimes')
    sales['created_at'] = pd.to_datetime(sales['created_at'])
    sales['last_updated'] = pd.to_datetime(sales['last_updated'])
    sales['agreed_payment_date'] = pd.to_datetime(sales['agreed_payment_date']).dt.date
    sales['agreed_delivery_date'] = pd.to_datetime(sales['agreed_delivery_date']).dt.date

    logger.info('[fact_sales_order] Separating date and time')

    sales['created_date'] = sales['created_at'].dt.date
    sales['created_time'] = sales['created_at'].dt.strftime('%H:%M:%S.%f')

    sales['last_updated_date'] = sales['last_updated'].dt.date
    sales['last_updated_time'] = sales['last_updated'].dt.strftime('%H:%M:%S.%f')

    logger.info('[fact_sales_order] Joining dates on created_at')
    created = sales.join(
        dates.rename(columns={'date_id': 'c_date_id', 'date': 'created_date'}),
        on='created_date',
        rsuffix='_2'
    )
    logger.info('[fact_sales_order] Joining dates on last_updated')
    updated = created.join(
        dates.rename(columns={'date_id': 'u_date_id', 'date': 'last_updated_date'}),
        on='last_updated_date',
        rsuffix='_2'
    )
    logger.info('[fact_sales_order] Joining dates on agreed_delivery_date')
    delivered = updated.join(
        dates.rename(columns={'date_id': 'd_date_id', 'date': 'agreed_delivery_date'}),
        on='agreed_delivery_date',
        rsuffix='_2'
    )
    logger.info('[fact_sales_order] Joining dates on payment_date')
    paymented = delivered.join(
        dates.rename(columns={'date_id': 'p_date_id', 'date': 'payment_date'}),
        on='agreed_payment_date',
        rsuffix='_2'
    )
    logger.info('[fact_sales_order] All dates joined')

    logger.info('[fact_sales_order] Selecting columns and renaming')
    processed_sales = paymented[[
        'sales_order_id',
        'c_date_id',
        'created_time',
        'u_date_id',
        'last_updated_time',
        'staff_id',
        'counterparty_id',
        'units_sold',
        'unit_price',
        'currency_id',
        'design_id',
        'p_date_id',
        'd_date_id',
        'agreed_delivery_location_id'
    ]]
    processed_sales.rename(
        columns={
            'c_date_id': 'created_date',
            'u_date_id': 'last_updated_date',
            'staff_id': 'sales_staff_id',
            'p_date_id': 'agreed_payment_date',
            'd_date_id': 'agreed_delivery_date'
        },
        inplace=True
    )
    processed_sales.insert(0, 'sales_record_id', range(1, len(processed_sales)+1))

    logger.info('[fact_sales_order] Table created')
    return processed_sales


# {'updates': ['currency', 'payment']}}
def lambda_handler(event, context):
    logger.info('Starting lambda')

    updates = event['updates']
    logger.info(f'Collecting updates from ingestion lambda for tables: {', '.join(updates)}')

    dimensions = {}
    facts = {}
    
    s3_client = boto3.client('s3')

    if 'counterparty' in updates:
        dimensions['dim_counterparty'] = make_dim_counterparty(s3_client)
    if 'currency' in updates:
        dimensions['dim_currency'] = make_dim_currency(s3_client)
    if 'design' in updates:
        dimensions['dim_design'] = make_dim_design(s3_client)
    if 'address' in updates:
        dimensions['dim_location'] = make_dim_location(s3_client)
    if 'payment_type' in updates:
        dimensions['dim_payment_type'] = make_dim_payment_type(s3_client)
    if 'staff' in updates or 'department' in updates:
        dimensions['dim_staff'] = make_dim_staff(s3_client)
    if 'transaction' in updates:
        dimensions['dim_transaction'] = make_dim_transaction(s3_client)

    if 'payment' in updates or 'purchase_order' in updates or 'sales_order' in updates:
        logger.info('Getting facts tables for date assessment')
        payment = get_from_ingest(s3_client, 'payment', '[payment]')
        purchase_order = get_from_ingest(s3_client, 'purchase_order', '[purchase_order]')
        sales_order = get_from_ingest(s3_client, 'sales_order', '[sales_order]')
        dimensions['dim_date'] = make_dim_dates(payment, purchase_order, sales_order)

        logger.info('Converting dates into a more easy format for facts')
        organised_dates = dimensions['dim_date'].copy()
        organised_dates['date'] = pd.to_datetime(organised_dates[['year', 'month', 'day']]).dt.date
        dates = organised_dates[['date_id', 'date']]
        dates.set_index('date', inplace=True)

        if 'payment' in updates:
            facts['fact_payment'] = make_fact_payment(payment, dates)
        if 'purchase_order' in updates:
            facts['fact_purchase_order'] = make_fact_purchase_order(purchase_order, dates)
        if 'sales_order' in updates:
            facts['fact_sales_order'] = make_fact_sales_order(sales_order, dates)

    if len(dimensions.keys()) > 0:
        logger.info('Uploading dimensions to processed bucket')
        for table in dimensions.keys():
            put_in_processed(s3_client, table, dimensions[table])
    else:
        logger.info('No dimensions to upload')

    if len(facts.keys()) > 0:
        logger.info('Uploading facts to processed bucket')
        for table in facts.keys():
            put_in_processed(s3_client, table, facts[table])
    else:
        logger.info('No facts to upload')

    new_files = list(dimensions.keys()) + list(facts.keys())
    new_files = [file.replace("_", "-") + '.parquet' for file in new_files]
    logger.info(f'Files updated: {', '.join(new_files)}')
    
    logger.info('Triggering warehousing lambda')
    try:
        lambda_client = boto3.client('lambda')
        lambda_client.invoke(
            FunctionName='warehousing_lambda',
            InvocationType='Event',
            Payload=json.dumps({'Records': new_files})
        )
    except ClientError as e:
        logger.error('Unable to trigger lambda')
        raise e
    logger.info('Processing lambda complete')