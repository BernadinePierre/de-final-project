resource "aws_iam_role" "warehouse_lambda" {
    name = "warehouse-lambda"
    assume_role_policy = <<EOF
    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "sts:AssumeRole"
                ],
                "Principal": {
                    "Service": [
                        "lambda.amazonaws.com"
                    ]
                }
            }
        ]
    }
    EOF
}

resource "aws_iam_role_policy_attachment" "warehouse_lambda_s3_readonly_policy_attachment" {
    role = aws_iam_role.warehouse_lambda.name
    policy_arn = aws_iam_policy.s3_process_readonly_policy.arn
}

resource "aws_iam_role_policy_attachment" "warehouse_lambda_cw_policy_attachment" {
    role = aws_iam_role.warehouse_lambda.name
    policy_arn = aws_iam_policy.cw_policy.arn
}

resource "aws_iam_role_policy_attachment" "lambda_vpc_policy_attachment" {
  role       = aws_iam_role.warehouse_lambda.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole"
}

resource "aws_iam_role_policy_attachment" "lambda_rds_policy_attachment" {
  role       = aws_iam_role.warehouse_lambda.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonRDSFullAccess"
}

resource "aws_lambda_permission" "s3_invoke" {
  statement_id  = "AllowS3Invoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.warehousing.arn
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.processed_bucket.arn
}

resource "aws_s3_bucket_notification" "processed_to_lambda" {
  bucket = aws_s3_bucket.processed_bucket.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.warehousing.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "processed-dim-"
    filter_suffix       = ".parquet"
  }

  depends_on = [aws_lambda_permission.s3_invoke]
}

data "aws_route_table" "main" {
  vpc_id = aws_vpc.rds_vpc.id
}

resource "aws_vpc_endpoint" "s3" {
  vpc_id            = aws_vpc.rds_vpc.id
  service_name      = "com.amazonaws.eu-west-2.s3"
  vpc_endpoint_type = "Gateway"
  
  route_table_ids = [data.aws_route_table.main.id]
}
