locals {
    ingest_lambda_script = "${path.module}/../src/ingestion_lambda.py"
    ingest_lambda_zip    = "${path.module}/lambdas/ingestion_lambda.zip"
    lambda_layer = "${path.module}/libraries/layer.zip"
}