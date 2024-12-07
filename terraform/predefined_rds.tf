resource "aws_db_instance" "trend_getter_predefined_db" {
  allocated_storage    = 20
  engine               = "postgres"
  instance_class       = var.db_instance_class
  db_name              = var.DB_NAME_2
  username             = var.DB_USERNAME
  password             = var.DB_PASSWORD
  publicly_accessible  = true
  skip_final_snapshot  = true
  storage_type         = "gp2"
  db_subnet_group_name   = "c14-public-subnet-group"
  vpc_security_group_ids = [aws_security_group.rds_sg.id]

  identifier = "c14-trend-getter-predefined-db"

  deletion_protection = false

  multi_az             = false
  backup_retention_period = 7
}
