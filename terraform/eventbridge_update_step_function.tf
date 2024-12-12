data "aws_sfn_state_machine" "my_state_machine" {
  name = "MyStateMachine-3czm3doz8"
}

resource "aws_cloudwatch_event_rule" "step_function_schedule_rule" {
  name                = "c14-trendgineers-hourly-step-function-schedule"
  description         = "Runs the Step Function every day at 6 PM"
  schedule_expression = "cron(0 * * * ? *)" # Cron for on each hour
}

resource "aws_iam_role" "eventbridge_step_function_role" {
  name = "EventBridgeStepFunctionRole"

  assume_role_policy = jsonencode({
    Version: "2012-10-17",
    Statement: [
      {
        Effect: "Allow",
        Principal: {
          Service: "events.amazonaws.com"
        },
        Action: "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_role_policy" "eventbridge_step_function_policy" {
  role = aws_iam_role.eventbridge_step_function_role.name

  policy = jsonencode({
    Version: "2012-10-17",
    Statement: [
      {
        Effect: "Allow",
        Action: "states:StartExecution",
        Resource: data.aws_sfn_state_machine.my_state_machine.arn
      }
    ]
  })
}

resource "aws_cloudwatch_event_target" "step_function_target" {
  rule      = aws_cloudwatch_event_rule.step_function_schedule_rule.name
  target_id = "step-function-daily-target"
  arn       = data.aws_sfn_state_machine.my_state_machine.arn

  input = jsonencode({
    "TriggerSource": "EventBridge",
    "ExecutionTime": "${timestamp()}"
  })

  role_arn = aws_iam_role.eventbridge_step_function_role.arn
}