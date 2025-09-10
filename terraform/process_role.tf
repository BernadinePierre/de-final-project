resource "aws_iam_role" "process_lambda" {
    name = "process-lambda"
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

resource "aws_iam_role_policy_attachment" "process_lambda_s3_readonly_policy_attachment" {
    role = aws_iam_role.process_lambda.name
    policy_arn = aws_iam_policy.s3_ingest_readonly_policy.arn
}

resource "aws_iam_role_policy_attachment" "process_lambda_s3_policy_attachment" {
    role = aws_iam_role.process_lambda.name
    policy_arn = aws_iam_policy.s3_process_policy.arn
}

resource "aws_iam_role_policy_attachment" "process_lambda_cw_policy_attachment" {
    role = aws_iam_role.process_lambda.name
    policy_arn = aws_iam_policy.cw_policy.arn
}

resource "aws_iam_role_policy_attachment" "process_lambda_invoke_policy_attachment" {
    role = aws_iam_role.process_lambda.name
    policy_arn = aws_iam_policy.lambda_invoke_policy.arn
}