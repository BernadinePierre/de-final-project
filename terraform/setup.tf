resource "aws_s3_bucket" "ingest_bucket" {
    bucket = "nc-crigglestone-ingest-bucket"
    object_lock_enabled = true
}

resource "aws_s3_bucket" "processed_bucket" {
    bucket = "nc-crigglestone-processed-bucket"
    object_lock_enabled = true
}

resource "aws_s3_object" "ingestion_lambda" {
    bucket= "nc-crigglestone-ingest-bucket"
    key = "lambda/ingestion_lambda.zip"
    source="${path.module}/ingestion_lambda.zip"
}

resource "aws_s3_object" "ingestion_lambda_layer" {
    bucket= "nc-crigglestone-ingest-bucket"
    key="lambda/layers/ingestion_layer.zip"
    source="${path.module}/../layers/layer.zip"

}

resource "aws_lambda_function" "ingestion" {
  function_name = "ingestion_lambda"
  role          = aws_iam_role.ingest_lambda.arn
  handler       = "ingestion_lambda.ingestion_lambda_handler"
  runtime       = "python3.13"
  # Store code in S3 instead of local zip

  s3_bucket = "nc-crigglestone-ingest-bucket"
  s3_key    = "lambda/ingestion_lambda.zip"

  layers = [
    aws_lambda_layer_version.ingestion_layer.arn
]
#   environment {
#     variables = {
#       S3_BUCKET   = "ingestion"
#       SECRET_NAME = "Project"
#       AWS_REGION  = "eu-west-2"
#     }
#   }
  timeout     = 60
  memory_size = 512
}

resource "aws_lambda_layer_version" "ingestion_layer" {
# filename = "/layers/layer.zip"
layer_name = "ingestion_layer"
compatible_runtimes = ["python3.13"]

# If storing in S3:
s3_bucket = "nc-crigglestone-ingest-bucket"
s3_key = "lambda/layers/ingestion_layer.zip"





}

data "archive_file" "ingestion_lambda" {
    source_file = "${path.module}/../src/ingestion_lambda.py"
    output_path = "${path.module}/ingestion_lambda.zip"
    type = "zip"
}