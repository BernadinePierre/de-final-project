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

resource "aws_iam_role_policy" "lambda_secretsmanager_policy" {
  name = "lambda-secretsmanager-policy"
  role = aws_iam_role.warehouse_lambda.name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "secretsmanager:GetSecretValue"
        ]
        Resource = "arn:aws:secretsmanager:eu-west-2:150327910081:secret:warehouse-db-credentials*"
      }
    ]
  })
}

resource "aws_iam_policy" "secrets_policy" {
    name = "secrets-policy"
    policy = data.aws_iam_policy_document.secrets_document.json
}

# --- LAMBDA BUCKET POLICIES ---

data "aws_iam_policy_document" "s3_data_updates_document" {
    statement {
        actions = ["s3:ListBucket"]

        resources = ["${aws_s3_bucket.lambda_bucket.arn}"]
    }
    
    statement {
        actions = ["s3:*"]

        resources = ["${aws_s3_bucket.lambda_bucket.arn}/*",]
    }
}

resource "aws_iam_policy" "s3_data_updates_policy" {
    name = "s3-data-updates-policy"
    policy = data.aws_iam_policy_document.s3_data_updates_document.json
}

# --- LAMBDA INVOKE POLICIES ---

data "aws_iam_policy_document" "lambda_invoke_document" {
    statement {
        actions = ["lambda:InvokeFunction"]
        
        resources = ["*"]
    }
}

resource "aws_iam_policy" "lambda_invoke_policy" {
    name = "lambda-invocation-policy"
    policy = data.aws_iam_policy_document.lambda_invoke_document.json
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

data "aws_iam_policy_document" "s3_ingest_readonly_document" {
    statement {
        actions = [
            "s3:Get*",
            "s3:List*",
            "s3:Describe*",
            "s3-object-lambda:Get*",
            "s3-object-lambda:List*"
        ]

        resources = [
            "${aws_s3_bucket.ingest_bucket.arn}/*"
        ]
    }

    statement {
        actions = [
            "s3:ListBucket"
        ]

        resources = [
            "${aws_s3_bucket.ingest_bucket.arn}",
        ]
    }
}

resource "aws_iam_policy" "s3_ingest_policy" {
    name = "s3-ingest-policy"
    policy = data.aws_iam_policy_document.s3_ingest_document.json
}

resource "aws_iam_policy" "s3_ingest_readonly_policy" {
    name = "s3-ingest-readonly-policy"
    policy = data.aws_iam_policy_document.s3_ingest_readonly_document.json
}

# --- PROCESS POLICIES ---

data "aws_iam_policy_document" "s3_process_document" {
    statement {
        actions = ["s3:*"]

        resources = ["${aws_s3_bucket.processed_bucket.arn}/*"]
    }
}

data "aws_iam_policy_document" "s3_process_readonly_document" {
    statement {
        actions = [
            "s3:Get*",
            "s3:List*",
            "s3:Describe*",
            "s3-object-lambda:Get*",
            "s3-object-lambda:List*"
        ]

        resources = ["${aws_s3_bucket.processed_bucket.arn}/*"]
    }
}

resource "aws_iam_policy" "s3_process_policy" {
    name = "s3-process-policy"
    policy = data.aws_iam_policy_document.s3_process_document.json
}

resource "aws_iam_policy" "s3_process_readonly_policy" {
    name = "s3-process-readonly-policy"
    policy = data.aws_iam_policy_document.s3_process_readonly_document.json
}

# --- WAREHOUSE POLICIES ---

resource "aws_iam_role_policy_attachment" "lambda_s3_access" {
  role       = aws_iam_role.warehouse_lambda.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess"
}

resource "aws_iam_role_policy_attachment" "lambda_rds_access" {
  role       = aws_iam_role.warehouse_lambda.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonRDSFullAccess"
}
