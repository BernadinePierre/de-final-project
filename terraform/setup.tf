# --- BUCKETS ---

resource "aws_s3_bucket" "lambda_bucket" {
    bucket = "nc-crigglestone-lambda-bucket"
}

resource "aws_s3_bucket" "ingest_bucket" {
    bucket = "nc-crigglestone-ingest-bucket"
    object_lock_enabled = true
}

resource "aws_s3_bucket" "processed_bucket" {
    bucket = "nc-crigglestone-processed-bucket"
    object_lock_enabled = true
}

# --- LAMBDA LAYER ---

resource "aws_s3_object" "lambda_layer" {
    bucket = aws_s3_bucket.lambda_bucket.id
    key = "layers/lambda_layer.zip"
    source = local.lambda_layer
}

resource "aws_lambda_layer_version" "lambda_layer" {
    layer_name = "lambda-layer"
    compatible_runtimes = ["python3.13"]

    s3_bucket = aws_s3_bucket.lambda_bucket.bucket
    s3_key = aws_s3_object.lambda_layer.key
}

# --- INGESTION LAMBDA ---

data "archive_file" "ingest_lambda_zip" {
    source_file = local.ingest_lambda_script
    output_path = local.ingest_lambda_zip
    type = "zip"
}

resource "aws_s3_object" "ingestion_lambda" {
    bucket = aws_s3_bucket.lambda_bucket.id
    key = "lambda/ingestion_lambda.zip"
    source = local.ingest_lambda_zip
}

resource "aws_lambda_function" "ingestion" {
    function_name = "ingestion_lambda"
    role          = aws_iam_role.ingest_lambda.arn
    handler       = "ingestion_lambda.ingestion_lambda_handler"
    runtime       = "python3.13"

    s3_bucket = aws_s3_bucket.lambda_bucket.bucket
    s3_key    = aws_s3_object.ingestion_lambda.key

    layers = [
        aws_lambda_layer_version.lambda_layer.arn
    ]
    
    timeout     = 60
    memory_size = 512
}