
resource "aws_iam_role" "dashboard_ecs_service_role" {
  name               = "c14-trendgineers-dashboard-ecs-service-role"
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

resource "aws_iam_policy" "dashboard_ecs_service_rds_policy" {
  name        = "c14-trendgineers-dashboard-ecs-service-rds-policy"
  description = "Grant ECS service permissions to interact with the RDS database for reading and writing data."
  lifecycle {
    prevent_destroy = false
  }

  policy = jsonencode({
    Version : "2012-10-17",
    Statement : [
      {
        Effect   : "Allow",
        Action   : [
          "rds:DescribeDBInstances"
        ],
        Resource : [
            "arn:aws:rds:eu-west-2:129033205317:db:c14-trend-getter-db"
        ]
      },
      # Allow ECS tasks to read from the specified S3 bucket
      {
        Effect   : "Allow",
        Action   : [
          "s3:GetObject",     
          "s3:ListBucket"      
        ],
        Resource : [
          "arn:aws:s3:::trendgineers-raw-firehose-data",      
          "arn:aws:s3:::trendgineers-raw-firehose-data/*"     
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "dashboard_ecs_service_s3_policy_attachment" {
  role       = aws_iam_role.dashboard_ecs_service_role.name
  policy_arn = aws_iam_policy.dashboard_ecs_service_rds_policy.arn
}

resource "aws_cloudwatch_log_group" "dashboard_log_group" {
  name              = "/ecs/c14-trendgineers-dashboard"
  lifecycle {
    prevent_destroy = false
  }
  retention_in_days = 7
}

resource "aws_ecs_task_definition" "c14_trendgineers_dashboard" {
  family                   = "c14-trendgineers-dashboard"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = "256"
  memory                   = "1024"
  task_role_arn            = aws_iam_role.dashboard_ecs_service_role.arn # keep
  execution_role_arn       = data.aws_iam_role.ecs_task_execution_role.arn

  container_definitions = jsonencode([
    {
      name        = "c14-trendgineers-dashboard" 
      image       = "129033205317.dkr.ecr.eu-west-2.amazonaws.com/c14-trendgineers-dashboard-ecr:latest" # change to our dashboard image
      cpu         = 256
      memory      = 512
      essential   = true
      
      environment = [
        { name = "DB_HOST", value = var.DB_HOST },
        { name = "DB_PORT", value = var.DB_PORT },
        { name = "DB_USERNAME", value = var.DB_USERNAME },
        { name = "SCHEMA_NAME", value = var.SCHEMA_NAME },
        { name = "DB_NAME", value = var.DB_NAME },
        { name = "DB_PASSWORD", value = var.DB_PASSWORD },
        { name = "AWS_ACCESS_KEY_ID", value = var.ACCESS_KEY_ID },
        { name = "AWS_SECRET_ACCESS_KEY", value = var.SECRET_ACCESS_KEY }
      ]

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-create-group  = "true"
          awslogs-group         = "/ecs/c14-trendgineers-dashboard"
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

resource "aws_security_group" "dashboard_ecs_service_sg" {
  name        = "c14-trendgineers-dashboard-sg"
  description = "Allow ECS tasks to read data from S3 and upload to RDS"
  vpc_id      = var.VPC_ID

  lifecycle {
    prevent_destroy = false
  }

  # Egress rule for S3 (HTTPS)
  egress {
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"] 
  }

  # Egress rule for RDS
  egress {
    from_port   = 5432 # Replace with your RDS port if different
    to_port     = 5432
    protocol    = "tcp"
    security_groups = [aws_security_group.rds_sg.id] 
  }

  tags = {
    Name = "dashboard ECS Service SG"
  }
}

resource "aws_ecs_service" "dashboard_service" {
  name            = "c14-trendgineers-dashboard-service"
  cluster         = data.aws_ecs_cluster.cluster.id
  task_definition = aws_ecs_task_definition.c14_trendgineers_dashboard.arn
  desired_count   = 1
  launch_type     = "FARGATE"

  network_configuration {
    subnets         = ["subnet-0497831b67192adc2",
                "subnet-0acda1bd2efbf3922",
                "subnet-0465f224c7432a02e"] 
    security_groups = [aws_security_group.dashboard_ecs_service_sg.id]
    assign_public_ip = true
  }
}