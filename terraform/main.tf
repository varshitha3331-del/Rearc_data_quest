########################################
# Provider + basic config
########################################

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = "us-east-1"
}

########################################
# Input: your existing S3 bucket name
########################################

variable "rearc_bucket" {
  type        = string
  description = "Existing S3 bucket used for Rearc Data Quest"
}

########################################
# IAM Role for Lambda A (ingest)
########################################

resource "aws_iam_role" "lambda_ingest_role" {
  name = "rearc-lambda-ingest-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Principal = {
          Service = "lambda.amazonaws.com"
        },
        Action = "sts:AssumeRole"
      }
    ]
  })
}

# Basic CloudWatch Logs permissions
resource "aws_iam_role_policy_attachment" "lambda_ingest_logs" {
  role       = aws_iam_role.lambda_ingest_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

# Allow Lambda A to read/write from your bucket
resource "aws_iam_role_policy" "lambda_ingest_s3_policy" {
  name = "rearc-lambda-ingest-s3-policy"
  role = aws_iam_role.lambda_ingest_role.id

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:ListBucket"
        ],
        Resource = [
          "arn:aws:s3:::${var.rearc_bucket}",
          "arn:aws:s3:::${var.rearc_bucket}/*"
        ]
      }
    ]
  })
}

########################################
# Lambda A: Ingest (Part 1 + Part 2)
########################################

resource "aws_lambda_function" "lambda_ingest" {
  function_name = "rearc-data-quest-ingest"
  role          = aws_iam_role.lambda_ingest_role.arn
  handler       = "main.lambda_handler"
  runtime       = "python3.9"
  timeout       = 900

  # Path to the zip we built in your project root
  filename         = "${path.module}/../lambda_ingest.zip"
  source_code_hash = filebase64sha256("${path.module}/../lambda_ingest.zip")

  environment {
    variables = {
      REARC_BUCKET  = var.rearc_bucket
      BLS_BASE      = "https://download.bls.gov/pub/time.series/pr/"
      BLS_INDEX     = "https://download.bls.gov/pub/time.series/pr/"
      REARC_POP_KEY = "rearc-data-quest/population/us_population_all_years.json"
    }
  }
}

########################################
# CloudWatch Event Rule: run Lambda A daily
########################################

resource "aws_cloudwatch_event_rule" "lambda_ingest_daily" {
  name                = "rearc-data-quest-ingest-daily"
  description         = "Run ingest lambda once per day"
  schedule_expression = "rate(1 day)"
}

resource "aws_cloudwatch_event_target" "lambda_ingest_target" {
  rule      = aws_cloudwatch_event_rule.lambda_ingest_daily.name
  target_id = "lambda-ingest"
  arn       = aws_lambda_function.lambda_ingest.arn
}

# Allow Events to invoke the Lambda
resource "aws_lambda_permission" "allow_events_to_invoke_lambda_ingest" {
  statement_id  = "AllowExecutionFromCloudWatch"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.lambda_ingest.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.lambda_ingest_daily.arn
}

########################################
# SQS queue for analytics trigger
########################################


resource "aws_sqs_queue" "analytics_queue" {
  name                      = "rearc-data-quest-analytics-queue"
  visibility_timeout_seconds = 910
}


# Allow S3 bucket to send messages to this queue
resource "aws_sqs_queue_policy" "analytics_queue_policy" {
  queue_url = aws_sqs_queue.analytics_queue.id

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Principal = {
          Service = "s3.amazonaws.com"
        },
        Action = "sqs:SendMessage",
        Resource = aws_sqs_queue.analytics_queue.arn,
        Condition = {
          ArnEquals = {
            "aws:SourceArn" = "arn:aws:s3:::${var.rearc_bucket}"
          }
        }
      }
    ]
  })
}

########################################
# S3 -> SQS notification for population JSON
########################################

resource "aws_s3_bucket_notification" "population_to_sqs" {
  bucket = var.rearc_bucket

  queue {
    queue_arn     = aws_sqs_queue.analytics_queue.arn
    events        = ["s3:ObjectCreated:Put"]
    filter_prefix = "rearc-data-quest/population/"
    filter_suffix = ".json"
  }

  depends_on = [aws_sqs_queue_policy.analytics_queue_policy]
}

########################################
# IAM Role for Lambda B (analytics)
########################################

resource "aws_iam_role" "lambda_analytics_role" {
  name = "rearc-lambda-analytics-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Principal = {
          Service = "lambda.amazonaws.com"
        },
        Action = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_analytics_logs" {
  role       = aws_iam_role.lambda_analytics_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy" "lambda_analytics_s3_sqs_policy" {
  name = "rearc-lambda-analytics-s3-sqs-policy"
  role = aws_iam_role.lambda_analytics_role.id

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Action = [
          "s3:GetObject",
          "s3:ListBucket"
        ],
        Resource = [
          "arn:aws:s3:::${var.rearc_bucket}",
          "arn:aws:s3:::${var.rearc_bucket}/*"
        ]
      },
      {
        Effect = "Allow",
        Action = [
          "sqs:ReceiveMessage",
          "sqs:DeleteMessage",
          "sqs:GetQueueAttributes"
        ],
        Resource = aws_sqs_queue.analytics_queue.arn
      }
    ]
  })
}

########################################
# Lambda B: Analytics (Part 3)
########################################

resource "aws_lambda_function" "lambda_analytics" {
  function_name = "rearc-data-quest-analytics"
  role          = aws_iam_role.lambda_analytics_role.arn
  handler       = "main.lambda_handler"
  runtime       = "python3.9"
  timeout       = 900

  filename         = "${path.module}/../lambda_analytics.zip"
  source_code_hash = filebase64sha256("${path.module}/../lambda_analytics.zip")

  environment {
    variables = {
      REARC_BUCKET = var.rearc_bucket
      BLS_KEY      = "rearc-data-quest/bls/pr.data.0.Current"
      POP_KEY      = "rearc-data-quest/population/us_population_all_years.json"
    }
  }
}

########################################
# SQS -> Lambda B event source mapping
########################################

resource "aws_lambda_event_source_mapping" "sqs_to_lambda_analytics" {
  event_source_arn  = aws_sqs_queue.analytics_queue.arn
  function_name     = aws_lambda_function.lambda_analytics.arn
  batch_size        = 1
  enabled           = true
}