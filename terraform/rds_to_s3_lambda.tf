# IAM Role for Lambda
resource "aws_iam_role" "rds_to_s3_lambda_role" {
  name               = "rds_to_s3_lambda_role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect    = "Allow",
        Principal = {
          Service = "lambda.amazonaws.com"
        },
        Action = "sts:AssumeRole"
      }
    ]
  })
}

# IAM Policy for Lambda
resource "aws_iam_role_policy" "rds_to_s3_lambda_policy" {
  name = "rds_to_s3_lambda_policy"
  role = aws_iam_role.rds_to_s3_lambda_role.id
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect   = "Allow",
        Action   = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ],
        Resource = "*"
      },
      {
        Effect   = "Allow",
        Action   = [
          "s3:PutObject",
          "s3:GetObject",
          "s3:ListBucket",
          "s3:DeleteObject",
          "s3:HeadObject"
        ],
        Resource = "*" # Reference existing bucket
      },
      {
        Effect   = "Allow",
        Action   = [
          "rds:Connect"
        ],
        Resource = "arn:aws:rds:eu-west-2:129033205317:db:c14-trend-getter-db" 
      }
    ]
  })
}

resource "aws_cloudwatch_log_group" "rds_to_s3_lambda_log_group" {
  name              = "/aws/lambda/c14-trendgineers-rds-to-s3-etl-lambda"
  retention_in_days = 7
}

# Lambda Function
resource "aws_lambda_function" "rds_to_s3_etl_lambda" {
  function_name = "c14-trendgineers-rds-to-s3-etl-lambda"
  role          = aws_iam_role.rds_to_s3_lambda_role.arn

  package_type  = "Image"
  architectures = ["x86_64"]
  image_uri = "129033205317.dkr.ecr.eu-west-2.amazonaws.com/c14-trendgineers-rds-to-s3-ecr@sha256:80ce6b0839817fc7abcabc740003ce68ecf934b9bca2dcdcb9e229376804308e"

  timeout       = 720
  depends_on    = [aws_cloudwatch_log_group.rds_to_s3_lambda_log_group]

  environment {
    variables = {
      SCHEMA_NAME    = var.SCHEMA_NAME
      S3_BUCKET_NAME = "trendgineers-test-bucket" # Reference existing bucket
      DB_USERNAME    = var.DB_USERNAME
      DB_PASSWORD    = var.DB_PASSWORD
      DB_HOST        = var.DB_HOST
      DB_PORT        = var.DB_PORT
      DB_NAME        = var.DB_NAME
      ACCESS_KEY_ID = var.ACCESS_KEY_ID
      SECRET_ACCESS_KEY = var.SECRET_ACCESS_KEY
    }
  }

  logging_config {
    log_format = "Text"
    log_group  = "/aws/lambda/c14-trendgineers-rds-to-s3-etl-lambda"
  }
  tracing_config {
    mode = "PassThrough"
  }

}

# # CloudWatch Event Rule for Schedule
# resource "aws_cloudwatch_event_rule" "schedule_rule" {
#   name        = "etl_lambda_schedule"
#   description = "Runs the ETL Lambda every 5 minutes"
#   schedule_expression = "rate(5 minutes)"
# }

# # Lambda Permission for CloudWatch Rule
# resource "aws_lambda_permission" "allow_cloudwatch" {
#   statement_id  = "AllowExecutionFromCloudWatch"
#   action        = "lambda:InvokeFunction"
#   function_name = aws_lambda_function.rds_to_s3_etl_lambda.function_name
#   principal     = "events.amazonaws.com"
#   source_arn    = aws_cloudwatch_event_rule.schedule_rule.arn
# }

# # CloudWatch Event Target
# resource "aws_cloudwatch_event_target" "lambda_target" {
#   rule      = aws_cloudwatch_event_rule.schedule_rule.name
#   target_id = "c14-trendgineers-rds-to-s3-etl-lambda"
#   arn       = aws_lambda_function.rds_to_s3_etl_lambda.arn
# }