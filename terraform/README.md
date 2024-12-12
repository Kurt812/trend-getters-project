# Terraform 🏗️

This directory focuses on **terraforming** all the AWS cloud services that are used throughout the Trend Getter Application.

## Files Explained 🗂️
- **`main.tf`**: this terraform file ensures AWS resources are created within the right availability zones.
- **`dashboard_ecr.tf`**: this terraform file creates an Elastic Container Repository (ECR) for storing the dashboard image. 
- **`keyword_notification_ecr.tf`**: this terraform file creates an Elastic Container Repository (ECR) for storing the script that handles updating the data for subscribed to words.
 - **`notification_ecr.tf`**: this terraform file creates an Elastic Container Repository (ECR) for storing the script that handles the sending of emails.
- **`pipeline_ecr.tf`**: this terraform file creates an Elastic Container Repository (ECR) for storing the pipeline image. 
- **`rds_to_s3_ecr.tf`**: this terraform file creates an Elastic Container Repository (ECR) for storing the script which updates the S3 bucket with long term data. 
- **`keyword_notification_update_ecs.tf`**: this terraform file creates an Elastic Container Service (ECS) outlining the specifications for how the related docker image should be run.
- **`pipeline_ecs.tf`**: this terraform file creates an Elastic Container Service (ECS) outlining the specifications for how the related docker image should be run.
- **`notifications_lambda.tf`**: this Terraform configuration file provisions resources for the c14-trendgineers-notifications-lambda function, responsible for sending personalized email notifications using AWS SES.
- **`rds_to_s3_lambda.tf`**: this Terraform configuration file provisions resources for the c14-trendgineers-rds-to-s3-etl-lambda function, designed to extract data from an RDS database and upload it to an S3 bucket. 
- **`upload_ecs.tf`**: this Terraform configuration file provisions the infrastructure for an ECS task that uploads raw Bluesky data to an S3 bucket. 
- **`eventbridge_update_step_function.tf`**: this Terraform configuration file sets up an EventBridge rule to trigger an AWS Step Function on a scheduled basis (hourly).
- **`rds.tf`**: this terraform file defines a security group for the RDS to allow traffic on SSH and PostgreSQL ports, and a PostgreSQL RDS instance linked to the former.
- **`variables.tf`**: this file acts as a blueprint for all the variables used in the Terraform configuration. It includes the definitions of the variables that store sensitive information, such as AWS credentials and other secret keys that are required for resource creation.

## Terraformed AWS services 💼
- 2 ECR Repositories - to store dockerised images of the pipeline and dashboard.
- Relational Database RDS - to store processed data.


## Installation ⚙️

Navigate to the project directory:
```bash
cd terraform
```

## Usage 🔄 
The main script to create AWS cloud services through terraform is `main.tf`. You can run it with the following command:

To initialise: 
```bash
terraform init 
```

To create the services:
```bash
terraform plan
terraform apply 
```
The ```apply``` command will prompt the user to enter ```yes``` to confirm the creation of the services. The services will then be created on AWS unless an error with any credentials has occurred. 

To destroy:
```bash
terraform destroy
```

## Secrets Management 🕵🏽‍♂️

Before running the script, you need to set up your AWS credentials. Create a new file called `.terraform.tfvars` in the `terraform` directory and add the following lines, with your actual AWS keys and database details:


| Variable          | Description                                            |
|------------------|--------------------------------------------------|
| ACCESS_KEY_ID          | 	The AWS access key ID for authenticating API requests.    |
| SECRET_ACCESS_KEY          | The AWS secret access key associated with the access key ID.  |
| S3_BUCKET_NAME      | The name of the S3 bucket where the files are stored.          |
| S3_OBJECT_PREFIX          | 	The prefix used enter sub-directories in the main S3 bucket.                 |
| VPC_ID           | The identifier for the Virtual Private Cloud (VPC) associated with the database. |
| DB_HOST          | The hostname or IP address of the database.      |
| DB_PORT          | The port number for the database connection.     |
| DB_PASSWORD      | The password for the database user.              |
| DB_USERNAME          | The username for the database.                   |
| DB_NAME          | The name of the database.                        |
| SCHEMA_NAME      | The name of the database schema.                 |

