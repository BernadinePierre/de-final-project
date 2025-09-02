resource "aws_scheduler_schedule" "ingest_scheduler" {
    name = "ingest-scheduler"
    group_name = "default"

    flexible_time_window {
        mode = "OFF"
    }

    schedule_expression = "rate(20 minutes)"

    target {
        arn = aws_lambda_function.ingestion.arn
        role_arn = aws_iam_role.scheduler_role.arn
    }
}

resource "aws_iam_role" "scheduler_role" {
    name = "scheduler"
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
                        "scheduler.amazonaws.com"
                    ]
                }
            }
        ]
    }
    EOF
}

data "aws_iam_policy_document" "scheduler_document" {
    statement {
        actions = ["lambda:InvokeFunction"]

        resources = ["*"]
    }
}

resource "aws_iam_policy" "scheduler_policy" {
    name = "scheduler-policy"
    policy = data.aws_iam_policy_document.scheduler_document.json
}

resource "aws_iam_role_policy_attachment" "scheduler_policy_attachment" {
    role = aws_iam_role.scheduler_role.name
    policy_arn = aws_iam_policy.scheduler_policy.arn
}