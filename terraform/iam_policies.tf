# --- CLOUDWATCH POLICY ---

data "aws_iam_policy_document" "cw_document" {
    statement {
        actions = [
            "logs:CreateLogStream",
            "logs:PutLogEvents",
        ]

        resources = [
            "*"
        ]
    }
}

resource "aws_iam_policy" "cw_policy" {
    name = "cw-policy"
    policy = data.aws_iam_policy_document.cw_document.json
}

# --- SECRETS POLICY ---

data "aws_iam_policy_document" "secrets_document" {
    statement {
        actions = ["secretsmanager:GetSecretValue"]

        resources = ["*"]
    }
}

resource "aws_iam_policy" "secrets_policy" {
    name = "secrets-policy"
    policy = data.aws_iam_policy_document.secrets_document.json
}

# --- INGEST POLICIES ---

data "aws_iam_policy_document" "s3_ingest_document" {
    statement {
        actions = ["s3:*"]

        resources = [
            "${aws_s3_bucket.ingest_bucket.arn}/*",
        ]
    }
}

resource "aws_iam_policy" "s3_ingest_policy" {
    name = "s3-ingest-policy"
    policy = data.aws_iam_policy_document.s3_ingest_document.json
}