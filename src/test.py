from ingestion_lambda import lambda_handler

if __name__ == "__main__":
    # Fake Lambda event + context
    event = {}
    context = {}
    lambda_handler(event, context)
