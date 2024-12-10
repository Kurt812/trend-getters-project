resource "aws_ecr_repository" "c14_trendgineers_notifications_ecr" {
  encryption_configuration {
    encryption_type = "AES256"
  }

  image_scanning_configuration {
    scan_on_push = false
  }

  image_tag_mutability = "MUTABLE"
  name                 = "c14-trendgineers-notifications-ecr"
  lifecycle {
    prevent_destroy = false
  }
}