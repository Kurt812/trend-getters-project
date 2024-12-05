# Terraform 🏗️

This directory focuses on **terraforming** all the AWS cloud services that are used throughout the Trend Getter Application.

## Files Explained 🗂️
- **`dashboard_ecr.tf`**: this terraform file creates an Elastic Container Repository (ECR) for storing the dashboard image. 
- **`main.tf`**: this terraform file ensures AWS resources are created within the right availability zones.
- **`pipeline_ecr.tf`**: this terraform file creates an Elastic Container Repository (ECR) for storing the pipeline image. 
- **`rds.tf`**: this terraform file defines a security group for the RDS to allow traffic on SSH and PostgreSQL ports, and a PostgreSQL RDS instance linked to the former.
- **`requirements.txt`**: this project requires specific Python libraries to run correctly. These dependencies are listed in this file and are needed to ensure your environment matches the project's environment requirements.
- **`variables.tf`**: tthis file acts as a blueprint for all the variables used in the Terraform configuration. It includes the definitions of the variables that store sensitive information, such as AWS credentials and other secret keys that are required for resource creation.

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
|-------------------|--------------------------------------------------------|
| VPC_ID        | The identifier for the Virtual Private Cloud (VPC) associated with the database.                  |
| DB_USERNAME          | The username for the database.                         |
| DB_PASSWORD       | The password for the database user.                    |
| DB_NAME           | The name of the database.                              |
| DB_HOST           | The hostname or IP address of the database.            |
| DB_PORT           | The port number for the database connection.           |
| DB_INSTANCE_CLASS      | The instance type for the RDS database.                       |

