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

variable "db_username" {
  description = "The database username"
  type        = string
  sensitive = true
}

variable "db_password" {
  description = "The database password"
  type        = string
  sensitive = true
}

variable "db_port" {
  description = "The database password"
  type        = string
  sensitive = true
}

variable "db_host" {
  description = "The database endpoint"
  type        = string
  sensitive = true
}

variable "db_instance_class" {
  description = "The instance type for the RDS instance"
  type        = string
  default     = "db.t3.micro"
}
