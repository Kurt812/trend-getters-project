# Terraform 🏗️

This directory focuses on **terraforming** all the AWS cloud services that are used throughout the Trend Getter Application.

## Files Explained 🗂️



## Terraformed AWS services 💼

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
| AWS_ACCESS_KEY    | The access key for AWS authentication.                 |
