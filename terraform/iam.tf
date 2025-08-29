resource "aws_iam_role" "ingest_lambda" {
    name = "ingest-lambda"
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

data "aws_iam_policy_document" "s3_ingest_document" {
    statement {
        actions = ["s3:*"]

        resources = [
            "${aws_s3_bucket.ingest_bucket.arn}/*",
        ]
    }
}

data "aws_iam_policy_document" "cw_ingest_document" {
    statement {
        actions = [
            # "logs:CreateLogGroup",
            # "logs:CreateLogStream",
            # "logs:PutLogEvents",
            "logs:*"
        ]

        resources = [
            #aws_cloudwatch_log_group.ingestion_lambda_logs.arn
            "*"
        ]
    }
}

data "aws_iam_policy_document" "secrets_ingest_document" {
    statement {
        actions = ["secretsmanager:GetSecretValue"]

        resources = ["*"]
    }
}

resource "aws_iam_policy" "s3_ingest_policy" {
    name = "s3-ingest-policy"
    policy = data.aws_iam_policy_document.s3_ingest_document.json
}

resource "aws_iam_policy" "cw_ingest_policy" {
    name = "cw-ingest-policy"
    policy = data.aws_iam_policy_document.cw_ingest_document.json
}

resource "aws_iam_policy" "secrets_ingest_policy" {
    name = "secrets-ingest-policy"
    policy = data.aws_iam_policy_document.secrets_ingest_document.json
}

resource "aws_iam_role_policy_attachment" "ingest_lambda_s3_policy_attachment" {
    role = aws_iam_role.ingest_lambda.name
    policy_arn = aws_iam_policy.s3_ingest_policy.arn
}

resource "aws_iam_role_policy_attachment" "ingest_lambda_cw_policy_attachment" {
    role = aws_iam_role.ingest_lambda.name
    policy_arn = aws_iam_policy.cw_ingest_policy.arn
}

resource "aws_iam_role_policy_attachment" "ingest_lambda_secrets_policy_attachement" {
    role = aws_iam_role.ingest_lambda.name
    policy_arn = aws_iam_policy.secrets_ingest_policy.arn
}