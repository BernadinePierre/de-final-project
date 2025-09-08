import awswrangler as wr
import boto3
from botocore.exceptions import ClientError
import json
import pandas as pd
import logging
from datetime import datetime
import pg8000

logger = logging.getLogger()
logger.setLevel(logging.INFO)


tables = [
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

TABLE_LIST = {
    'address': [ # DONE
        'address_id',
        'address_line_1',
        'address_line_2',
        'district',
        'city',
        'postal_code',
        'country',
        'phone'
    ],
    'counterparty': [ # DONE
        'counterparty_id',
        'counterparty_legal_name',
        'legal_address_id',
        # 'commercial_contact',
        # 'delivery_contact'
    ],
    'currency': [ # CURRENCY NAME ??
        'currency_id',
        'currency_code'
    ],
    'department': [ # DONE
        'department_id',
        'department_name',
        'location',
        # 'manager'
    ],
    'design': [ # DONE
        'design_id',
        'design_name',
        'file_location',
        'file_name'
    ],
    'payment': [ # DONE
        'payment_id',
        'created_at',
        'last_updated',
        'transaction_id',
        'counterparty_id',
        'payment_amount',
        'currency_id',
        'payment_type_id',
        'paid',
        'payment_date',
        # 'company_ac_number',
        # 'counterparty_ac_number'
    ],
    'payment_type': [ # DONE
        'payment_type_id',
        'payment_type_name',
    ],
    'purchase_order': [ # DONE
        'purchase_order_id',
        'created_at',
        'last_updated',
        'staff_id',
        'counterparty_id',
        'item_code',
        'item_quantity',
        'item_unit_price',
        'currency_id',
        'agreed_delivery_date',
        'agreed_payment_date',
        'agreed_delivery_location_id'
    ],
    'sales_order': [ # DONE
        'sales_order_id',
        'created_at',
        'last_updated',
        'design_id',
        'staff_id',
        'counterparty_id',
        'units_sold',
        'unit_price',
        'currency_id',
        'agreed_delivery_date',
        'agreed_payment_date',
        'agreed_delivery_location_id'
    ],
    'staff': [ # DONE
        'staff_id',
        'first_name',
        'last_name',
        'department_id',
        'email_address',
    ],
    'transaction': [ # DONE
        'transaction_id',
        'transaction_type',
        'sales_order_id',
        'purchase_order_id'
    ]
}

# DATA_UPDATES = json.dumps({
#     "address": "0000-00-00 00:00:00.0",
#     "counterparty": "0000-00-00 00:00:00.0",
#     "currency": "0000-00-00 00:00:00.0",
#     "department": "0000-00-00 00:00:00.0",
#     "design": "0000-00-00 00:00:00.0",
#     "payment": "0000-00-00 00:00:00.0",
#     "payment_type": "0000-00-00 00:00:00.0",
#     "purchase_order": "0000-00-00 00:00:00.0",
#     "sales_order": "0000-00-00 00:00:00.0",
#     "staff": "0000-00-00 00:00:00.0",
#     "transaction": "0000-00-00 00:00:00.0"
# })
DATA_UPDATES = {table: "0000-00-00 00:00:00.0" for table in TABLE_LIST.keys()}


def get_secret() -> dict:
    secret_name = "Project"
    region_name = "eu-west-2"
    client = boto3.client(
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
        conn = pg8000.connect(
            host=database_info['host'],
            port=database_info['port'],
            database=database_info['database'],
            user=database_info['user'],
            password=database_info['password'],
            #sslrootcert='SSLCERTIFICATE'
        )
        logger.info(f'Database connection successful')
        return conn
    except Exception as e:
        logger.error(f'Database connection failed due to {e}')
        raise

def check_original_update(table_name, connection):
    logger.info('Getting latest update')
    query = f'SELECT last_updated::text AS last_updated FROM {table_name} ORDER BY last_updated DESC LIMIT 1'
    df = wr.postgresql.read_sql_query(sql=query, con=connection)
    logger.info(f'Table {table_name} last updated at {df['last_updated'].iloc[0]}')
    return df['last_updated'].iloc[0] if not df.empty else "0000-00-00 00:00:00.0"

def get_original_updates(table_name, connection, cutoff):
    logger.info('Getting updated data')
    query = f"SELECT {', '.join(TABLE_LIST[table_name])} FROM {table_name} WHERE last_updated::text > '{cutoff}'"
    df = wr.postgresql.read_sql_query(sql=query, con=connection)
    logger.info('Data fetched')
    return df

def put_in_s3(table, data, date):
    logger.info('Putting data into S3')
    path = f"s3://nc-crigglestone-ingest-bucket/{table}/{date}.csv"
    wr.s3.to_csv(df=data, path=path, index=False)
    logger.info(f'Table {table} updated into S3')

def get_updates_table(client):
    lambda_bucket = 'nc-crigglestone-lambda-bucket'
    key_name = 'update_tracking.json'
    try:
        logger.info('Checking update records')
        client.head_object(
            Bucket=lambda_bucket,
            Key=key_name
        )
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            logger.info('Update record file does not exist, creating one')
            client.put_object(
                Bucket=lambda_bucket,
                Key=key_name,
                Body=json.dumps(DATA_UPDATES)
            )
        else:
            raise
    finally:
        updates = client.get_object(
            Bucket=lambda_bucket,
            Key=key_name
        )
        return json.loads(updates['Body'].read().decode('utf-8'))


def lambda_handler(event, context):
    logger.info("Lambda ingestion job started")

    connection = connect_to_original_database()
    s3_client = boto3.client('s3')

    last_updated = get_updates_table(s3_client)
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M")
    logger.info(f"Creating files at time {current_time}")

    for table in TABLE_LIST.keys():
        logger.info(f'Starting table {table}')
        date = check_original_update(table, connection)

        if date > last_updated[table]:

            data = get_original_updates(table, connection, last_updated[table])
            put_in_s3(table, data, current_time)
            last_updated[table] = date
        else:
            logger.info(f"Table {table} does not have updates")
    
    logger.info('Updating records')
    s3_client.put_object(
        Bucket='nc-crigglestone-lambda-bucket',
        Key='update_tracking.json',
        Body=json.dumps(last_updated)
    )
    logger.info('Lambda ingestion completed')