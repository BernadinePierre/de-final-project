import pandas as pd

def ingestion_lambda_handler(event, context):
    table = pd.DataFrame([[0, 1, 2, 3, 4], [11, 4, 1234, 45, 676]])
    return {
        'status_code': 200,
        'data': table
    }

