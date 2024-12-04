data "aws_ecs_cluster" "cluster" {
  cluster_name = "c14-ecs-cluster"
}

data "aws_iam_role" "ecs_task_execution_role" {
  name = "ecsTaskExecutionRole"
}

resource "aws_iam_role" "pipeline_ecs_service_role" {
  name               = "c14-trendgineers-pipeline-ecs-service-role"
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

resource "aws_iam_policy" "pipeline_ecs_service_rds_policy" {
  name        = "c14-trendgineers-pipeline-ecs-service-rds-policy"
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
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "pipeline_ecs_service_s3_policy_attachment" {
  role       = aws_iam_role.pipeline_ecs_service_role.name
  policy_arn = aws_iam_policy.pipeline_ecs_service_rds_policy.arn
}

resource "aws_cloudwatch_log_group" "pipeline_log_group" {
  name              = "/ecs/c14-trendgineers-pipeline"
  lifecycle {
    prevent_destroy = false
  }
  retention_in_days = 7
}

resource "aws_ecs_task_definition" "c14_trendgineers_pipeline" {
  family                   = "c14-trendgineers-pipeline"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = "256"
  memory                   = "1024"
  task_role_arn            = aws_iam_role.pipeline_ecs_service_role.arn
  execution_role_arn       = data.aws_iam_role.ecs_task_execution_role.arn

  container_definitions = jsonencode([
    {
      name        = "c14-trendgineers-pipeline" 
      image       = "129033205317.dkr.ecr.eu-west-2.amazonaws.com/c14-trendgineers-pipeline-ecr:latest" # change
      cpu         = 256
      memory      = 512
      essential   = true
      portMappings = [ 
        {
          containerPort = 5432
          hostPort      = 5432
          protocol      = "tcp"
        }
      ]
      environment = [
        { name = "DATABASE_IP", value = var.DATABASE_IP },
        { name = "DATABASE_PORT", value = var.DATABASE_PORT },
        { name = "DATABASE_USERNAME", value = var.DATABASE_USERNAME },
        { name = "SCHEMA_NAME", value = var.db_schema },
        { name = "DATABASE_NAME", value = var.DATABASE_NAME },
        { name = "DATABASE_PASSWORD", value = var.DATABASE_PASSWORD }
      ]

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = "/ecs/c14-trendgineers-pipeline"
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

resource "aws_security_group" "pipeline_ecs_service_sg" {
  name = "c14-trendgineers-pipeline-sg"
  description = "Allow connection to Bluesky and Google trends"
  vpc_id      = var.vpc_id

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

resource "aws_ecs_service" "service" {
  name            = "c14-trendgineers-pipeline-service"
  cluster         = data.aws_ecs_cluster.cluster.id
  task_definition = aws_ecs_task_definition.c14_trendgineers_pipeline.arn
  desired_count   = 1
  launch_type     = "FARGATE"

  network_configuration {
    subnets         = ["subnet-0497831b67192adc2",
                "subnet-0acda1bd2efbf3922",
                "subnet-0465f224c7432a02e"] 
    security_groups = [aws_security_group.pipeline_ecs_service_sg.id]
    assign_public_ip = true
  }
}