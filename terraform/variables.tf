variable "DB_NAME" {
  description = "The name of the database to create"
  type        = string
  default     = "trends"
}

variable "VPC_ID" {
  description = "The VPC id we are using"
  type        = string
  sensitive = true
}

variable "DB_USERNAME" {
  description = "The database username"
  type        = string
  sensitive = true
}

variable "DB_PASSWORD" {
  description = "The database password"
  type        = string
  sensitive = true
}

variable "DB_PORT" {
  description = "The database port"
  type        = string
  sensitive = true
}

variable "DB_HOST" {
  description = "The database endpoint"
  type        = string
  sensitive = true
}

variable "SCHEMA_NAME" {
  description = "The database schema"
  type        = string
  sensitive = true
}

variable "S3_BUCKET_NAME" {
  description = "The bucket name"
  type        = string
  sensitive = true
}

variable "S3_FOLDER_NAME" {
  description = "The S3 folder name"
  type        = string
  sensitive = true
}

variable "S3_FILE_NAME" {
  description = "The S3 file name"
  type        = string
  sensitive = true
}

variable "S3_OBJECT_PREFIX" {
  description = "The s3 bucket folder"
  type        = string
  sensitive = true
}

variable "db_instance_class" {
  description = "The instance type for the RDS instance"
  type        = string
  default     = "db.t3.micro"
}

variable "ACCESS_KEY_ID" {
  description = "The AWS access key id"
  type        = string
  sensitive = true
}

variable "SECRET_ACCESS_KEY" {
  description = "The AWS secret access key"
  type        = string
  sensitive = true
}

variable "API_ENDPOINT" {
  description = "The dashboard API endpoint"
  type        = string
  sensitive = true
}
