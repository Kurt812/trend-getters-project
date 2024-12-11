# IAM Role for Lambda
resource "aws_iam_role" "notifications_lambda_role" {
  name               = "notifications_lambda_role"
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
resource "aws_iam_role_policy" "notifications_lambda_policy" {
  name = "notifications_lambda_policy"
  role = aws_iam_role.notifications_lambda_role.id
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
      "Effect": "Allow",
      "Action": [
        "ses:SendEmail",
        "ses:SendRawEmail"
      ],
      "Resource": "*"
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

resource "aws_cloudwatch_log_group" "notifications_lambda_log_group" {
  name              = "/aws/lambda/c14-trendgineers-notifications-lambda"
  retention_in_days = 7
}

# Lambda Function
resource "aws_lambda_function" "notifications_lambda" {
  function_name = "c14-trendgineers-notifications-lambda"
  role          = aws_iam_role.notifications_lambda_role.arn

  package_type  = "Image"
  architectures = ["x86_64"]
  image_uri = "129033205317.dkr.ecr.eu-west-2.amazonaws.com/c14-trendgineers-notifications-ecr@sha256:0d6369aff651753db6d59ca4cef22dc40fddccd70c301ccd1989ea21946c4d1b" # change

  timeout       = 720
  depends_on    = [aws_cloudwatch_log_group.notifications_lambda_log_group]

  environment {
    variables = {
      SCHEMA_NAME    = var.SCHEMA_NAME
      S3_BUCKET_NAME = var.S3_BUCKET_NAME
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
    log_group  = "/aws/lambda/c14-trendgineers-notifications-lambda"
  }
  tracing_config {
    mode = "PassThrough"
  }

}

# EventBridge Rule for Hourly Schedule
resource "aws_cloudwatch_event_rule" "notifications_schedule_rule" {
  name                = "notifications_lambda_schedule_hourly"
  description         = "Runs the notifications Lambda every hour"
  schedule_expression = "rate(1 hour)"  # Every hour
}

# Lambda Permission for EventBridge Rule
resource "aws_lambda_permission" "notifications_allow_eventbridge" {
  statement_id  = "AllowExecutionFromEventBridgeHourly"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.notifications_lambda.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.notifications_schedule_rule.arn
}

# EventBridge Target for Lambda
resource "aws_cloudwatch_event_target" "notifications_lambda_target" {
  rule      = aws_cloudwatch_event_rule.notifications_schedule_rule.name
  target_id = "notifications-lambda-hourly-target"
  arn       = aws_lambda_function.notifications_lambda.arn
}
