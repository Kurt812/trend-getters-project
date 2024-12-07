resource "aws_iam_role" "raw_upload_ecs_service_role" {
  name               = "c14-trendgineers-raw-upload-ecs-service-role"
  lifecycle {
    prevent_destroy = false
  }
  assume_role_policy = jsonencode({
    Version : "2012-10-17",
    Statement : [
      {
        Effect    : "Allow",
        Principal : {
          Service : "ecs-tasks.amazonaws.com"
        },
        Action    : "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_policy" "raw_upload_ecs_service_s3_policy" {
  name        = "c14-trendgineers-raw-upload-ecs-service-s3-policy"
  description = "Grant ECS service permissions to upload raw data to the specified S3 bucket."
  lifecycle {
    prevent_destroy = false
  }

  policy = jsonencode({
    Version : "2012-10-17",
    Statement : [
      {
        Effect   : "Allow",
        Action   : [
          "s3:PutObject",         # Allow uploading objects to the bucket
          "s3:PutObjectAcl",      # Allow setting ACLs (optional)
          "s3:ListBucket"         # Allow listing the bucket (optional, for validation)
        ],
        Resource : [
            "arn:aws:s3:::trendgineers-raw-firehose-data",       # Bucket itself (for ListBucket)
            "arn:aws:s3:::trendgineers-raw-firehose-data/*"     # Objects within the bucket
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "raw_upload_ecs_service_s3_policy_attachment" {
  role       = aws_iam_role.raw_upload_ecs_service_role.name
  policy_arn = aws_iam_policy.raw_upload_ecs_service_s3_policy.arn
}

resource "aws_cloudwatch_log_group" "raw_upload_log_group" {
  name              = "/ecs/c14-trendgineers-raw-upload"
  lifecycle {
    prevent_destroy = false
  }
  retention_in_days = 7
}

resource "aws_ecs_task_definition" "c14_trendgineers_raw_upload" {
  family                   = "c14-trendgineers-raw-upload"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = "256"
  memory                   = "1024"
  task_role_arn            = aws_iam_role.raw_upload_ecs_service_role.arn
  execution_role_arn       = data.aws_iam_role.ecs_task_execution_role.arn

  container_definitions = jsonencode([
    {
      name        = "c14-trendgineers-raw-upload" 
      image       = "129033205317.dkr.ecr.eu-west-2.amazonaws.com/c14-trendgineers-upload:latest" # change
      cpu         = 256
      memory      = 512
      essential   = true
      portMappings = [ 
        {
          name = "c14-trendgineers-container-80-tcp",
          containerPort = 80,
          hostPort = 80,
          protocol = "tcp",
          appProtocol = "http"
        }
      ]
      environment = [
        { name = "AWS_ACCESS_KEY_ID", value = var.ACCESS_KEY_ID },
        { name = "AWS_SECRET_ACCESS_KEY", value = var.SECRET_ACCESS_KEY }
      ]

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = "/ecs/c14-trendgineers-raw-upload"
          awslogs-region        = "eu-west-2"
          awslogs-stream-prefix = "ecs"
        }
      }
    }
  ])

  runtime_platform {
    cpu_architecture       = "X86_64"
    operating_system_family = "LINUX"
  }
}

resource "aws_security_group" "raw_upload_ecs_service_sg" {
  name = "c14-trendgineers-raw-upload-sg"
  description = "Allow connection to Bluesky and Google trends"
  vpc_id      = var.VPC_ID

  lifecycle {
    prevent_destroy = false
  }

  egress {
      from_port   = 443
      to_port     = 443
      protocol    = "tcp"
      cidr_blocks = ["0.0.0.0/0"] 
    }
}

# resource "aws_ecs_service" "service" {
#   name            = "c14-trendgineers-raw-upload-service"
#   cluster         = data.aws_ecs_cluster.cluster.id
#   task_definition = aws_ecs_task_definition.c14_trendgineers_raw_upload.arn
#   desired_count   = 1
#   launch_type     = "FARGATE"

#   network_configuration {
#     subnets         = ["subnet-0497831b67192adc2",
#                 "subnet-0acda1bd2efbf3922",
#                 "subnet-0465f224c7432a02e"] 
#     security_groups = [aws_security_group.raw_upload_ecs_service_sg.id]
#     assign_public_ip = true
#   }
# }