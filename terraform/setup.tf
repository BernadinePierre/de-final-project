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

data "archive_file" "lambda_layer_zip" {
    source_dir = "${path.module}/libraries/source"
    output_path = local.lambda_layer
    type = "zip"
}

resource "aws_s3_object" "lambda_layer" {
    bucket = aws_s3_bucket.lambda_bucket.id
    key = "layers/lambda_layer.zip"
    source = local.lambda_layer
}

resource "aws_lambda_layer_version" "lambda_layer" {
    layer_name = "lambda-layer"
    compatible_runtimes = [var.python_version]

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
    key = "lambda/${local.ingest_lambda_file}.zip"
    # source = local.ingest_lambda_zip
    source = data.archive_file.ingest_lambda_zip.output_path
    etag = filemd5(local.ingest_lambda_zip)
}

resource "aws_cloudwatch_log_group" "ingestion_lambda_logs" {
    name = "crigglestone/ingestion/standard_logs"
    retention_in_days = 14
}

resource "aws_lambda_function" "ingestion" {
    function_name = local.ingest_lambda_file
    role          = aws_iam_role.ingest_lambda.arn
    handler       = "${local.ingest_lambda_file}.lambda_handler"
    runtime       = var.python_version

    s3_bucket = aws_s3_bucket.lambda_bucket.bucket
    s3_key    = aws_s3_object.ingestion_lambda.key
    source_code_hash = data.archive_file.ingest_lambda_zip.output_base64sha256

    layers = [aws_lambda_layer_version.lambda_layer.arn]

    logging_config {
        log_format = "Text"
        log_group = aws_cloudwatch_log_group.ingestion_lambda_logs.name
    }

    timeout = 60
}

# --- PROCESS LAMBDA ---

data "archive_file" "process_lambda_zip" {
    source_file = local.process_lambda_script
    output_path = local.process_lambda_zip
    type = "zip"
}

resource "aws_s3_object" "process_lambda" {
    bucket = aws_s3_bucket.lambda_bucket.id
    key = "lambda/${local.process_lambda_file}.zip"
    source = local.process_lambda_zip
    etag = filemd5(local.process_lambda_zip)
}

resource "aws_cloudwatch_log_group" "process_lambda_logs" {
    name = "crigglestone/process/standard_logs"
    retention_in_days = 14
}

resource "aws_lambda_function" "processing" {
    function_name = local.process_lambda_file
    role          = aws_iam_role.process_lambda.arn
    handler       = "${local.process_lambda_file}.lambda_handler"
    runtime       = var.python_version

    s3_bucket = aws_s3_bucket.lambda_bucket.bucket
    s3_key    = aws_s3_object.process_lambda.key
    source_code_hash = data.archive_file.process_lambda_zip.output_base64sha256

    layers = [aws_lambda_layer_version.lambda_layer.arn]

    logging_config {
        log_format = "Text"
        log_group  = aws_cloudwatch_log_group.process_lambda_logs.name
    }
}

# --- WAREHOUSE LAMBDA ---

data "archive_file" "warehouse_lambda_zip" {
    source_file = local.warehouse_lambda_script
    output_path = local.warehouse_lambda_zip
    type = "zip"
}

resource "aws_s3_object" "warehouse_lambda" {
    bucket = aws_s3_bucket.lambda_bucket.id
    key = "lambda/${local.warehouse_lambda_file}.zip"
    source = local.warehouse_lambda_zip
    etag = filemd5(local.warehouse_lambda_zip)
}

resource "aws_cloudwatch_log_group" "warehouse_lambda_logs" {
    name = "crigglestone/warehouse/standard_logs"
    retention_in_days = 14
}

resource "aws_lambda_function" "warehousing" {
    function_name = local.warehouse_lambda_file
    role          = aws_iam_role.warehouse_lambda.arn
    handler       = "${local.warehouse_lambda_file}.lambda_handler"
    runtime       = var.python_version

    s3_bucket = aws_s3_bucket.lambda_bucket.bucket
    s3_key    = aws_s3_object.warehouse_lambda.key
    source_code_hash = data.archive_file.warehouse_lambda_zip.output_base64sha256

    layers = [aws_lambda_layer_version.lambda_layer.arn]

    logging_config {
        log_format = "Text"
        log_group  = aws_cloudwatch_log_group.warehouse_lambda_logs.name
    }
}