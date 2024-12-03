resource "aws_security_group" "rds_sg"{
    name = "c14-trend-getter-rds-sg"
    description = "Security group for trend getter RDS database."
    vpc_id = var.vpc_id

  ingress {
    description      = "Allow SSH"
    from_port        = 22
    to_port          = 22
    protocol         = "tcp"
    cidr_blocks      = ["0.0.0.0/0"] 
  }

  ingress {
    description      = "Allow"
    from_port        = 5432
    to_port          = 5432
    protocol         = "tcp"
    cidr_blocks      = ["0.0.0.0/0"] 
  }

  egress {
    from_port   = 0
    to_port     = 65535
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"] 
  }
} 


resource "aws_db_instance" "trend_getter_db" {
  allocated_storage    = 20
  engine               = "postgres"
  instance_class       = var.db_instance_class
  db_name              = var.db_name
  username             = var.db_username
  password             = var.db_password
  publicly_accessible  = true
  skip_final_snapshot  = true
  storage_type         = "gp2"
  db_subnet_group_name   = "c14-public-subnet-group"
  vpc_security_group_ids = [aws_security_group.rds_sg.id]

  identifier = "c14-trend-getter-db"

  deletion_protection = false

  multi_az             = false
  backup_retention_period = 7
}