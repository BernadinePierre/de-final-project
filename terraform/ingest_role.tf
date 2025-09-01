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

resource "aws_iam_role_policy_attachment" "ingest_lambda_s3_policy_attachment" {
    role = aws_iam_role.ingest_lambda.name
    policy_arn = aws_iam_policy.s3_ingest_policy.arn
}

resource "aws_iam_role_policy_attachment" "ingest_lambda_cw_policy_attachment" {
    role = aws_iam_role.ingest_lambda.name
    policy_arn = aws_iam_policy.cw_policy.arn
}

resource "aws_iam_role_policy_attachment" "ingest_lambda_secrets_policy_attachement" {
    role = aws_iam_role.ingest_lambda.name
    policy_arn = aws_iam_policy.secrets_policy.arn
}

resource "aws_iam_role_policy_attachment" "ingest_lambda_data_updates_policy_attachement" {
    role = aws_iam_role.ingest_lambda.name
    policy_arn = aws_iam_policy.s3_data_updates_policy.arn
}