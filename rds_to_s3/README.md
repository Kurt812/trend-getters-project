# Update S3 with data in RDS

This directory contains all the necessary code and documentation for the Extract Transform Load (ETL) pipeline used to update the long-term data stored in the S3 bucket.

## Requirements 📋

To run this script, you will need the following:
- `pytest`: For running unit tests.
- `pytest-cov`: For measuring test coverage.
- `pandas`: For data manipulation and analysis.
- `python-dotenv`: For loading environment variables from a `.env` file.
- `psycopg2-binary`: For connecting, querying and modifying the PostgreSQL database.
- `boto3`: For integration with AWS services, including uploading files and managing data in S3 buckets.
- `sqlalchemy`: For working with databases using SQL expressions, enabling seamless interaction and query building with PostgreSQL databases.


To install these dependencies, use the following command:

```zsh
pip3 install -r requirements.txt
```

## Files Explained 🗂️
- **`Dockerfile`**: this Dockerfile defines the setup for creating a container image compatible with AWS Lambda. It uses the latest Python Lambda base image from AWS, installs necessary dependencies and includes the etl_lambda.py script. The container is configured to use etl_lambda.lambda_handler as the entry point when executed.
- **`etl_lambda`**: This Python script implements an ETL pipeline to extract keyword recording data older than 24 hours from a PostgreSQL RDS database, transform it to remove duplicates, and upload the results to an S3 bucket as a CSV file. The script also deletes the processed data from the database and is designed to run on AWS Lambda.
- **`test_etl`**: this Python test script validates the functionality of etl_lambda.py, which handles moving data from an RDS database to an S3 bucket. It uses the pytest framework with mock testing to ensure components like database connections, S3 interactions, and file processing behave as expected. Key features tested include:
	•	Successful and failed connections to RDS and S3.
	•	Proper handling of data fetching, uploading, and local file management.
	•	Robust exception handling for edge cases like missing credentials or file errors.
	•	Validation of the lambda_handler function’s ability to orchestrate the ETL process.
- **`requirements.txt`**: this project requires specific Python libraries to run correctly. These dependencies are listed in this file and are needed to ensure your environment matches the project's environment requirements.

## Secrets Management 🕵🏽‍♂️
Before running the script, you need to set up your AWS credentials. Create a new file called `.env` in the `pipeline` directory and add the following lines, with your actual AWS keys and database details:

| Variable         | Description                                      |
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
