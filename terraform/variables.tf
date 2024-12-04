variable "DATABASE_NAME" {
  description = "The name of the database to create"
  type        = string
  default     = "trends"
}

variable "db_name" {
  description = "The name of the database to create"
  type        = string
  default     = "trends"
}

variable "vpc_id" {
  description = "The VPC id we are using"
  type        = string
  sensitive = true
}

variable "DATABASE_USERNAME" {
  description = "The database username"
  type        = string
  sensitive = true
}

variable "db_username" {
  description = "The database username"
  type        = string
  sensitive = true
}

variable "DATABASE_PASSWORD" {
  description = "The database password"
  type        = string
  sensitive = true
}

variable "db_password" {
  description = "The database password"
  type        = string
  sensitive = true
}

variable "DATABASE_PORT" {
  description = "The database port"
  type        = string
  sensitive = true
}

variable "DATABASE_IP" {
  description = "The database endpoint"
  type        = string
  sensitive = true
}

variable "db_schema" {
  description = "The database schema"
  type        = string
  sensitive = true
}

variable "db_instance_class" {
  description = "The instance type for the RDS instance"
  type        = string
  default     = "db.t3.micro"
}


