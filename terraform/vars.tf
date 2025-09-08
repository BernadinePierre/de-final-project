locals {
    lambda_layer = "${path.module}/libraries/layer.zip"

    ingest_lambda_file   = "ingestion_lambda"
    ingest_lambda_script = "${path.module}/../src/${local.ingest_lambda_file}.py"
    ingest_lambda_zip    = "${path.module}/lambdas/${local.ingest_lambda_file}.zip"

    process_lambda_file   = "process_lambda"
    process_lambda_script = "${path.module}/../src/${local.process_lambda_file}.py"
    process_lambda_zip    = "${path.module}/lambdas/${local.process_lambda_file}.zip"

    warehouse_lambda_file   = "warehousing_lambda"
    warehouse_lambda_script = "${path.module}/../src/${local.warehouse_lambda_file}.py"
    warehouse_lambda_zip    = "${path.module}/lambdas/${local.warehouse_lambda_file}.zip"
}

variable "python_version" {
    type = string
    default = "python3.13"
}

variable "warehouse_username" {}
variable "warehouse_password" {sensitive = true}