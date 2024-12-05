# Clean
This directory contains the necessary code to clean the S3 bucket after a retention period. 

## Requirements
To run this script, you will need the following:
- `certifi`: For providing valid SSL certificates for secure connection to firehose.
- `boto3`: For integration with AWS services, including uploading files and managing data in S3 buckets.
- `python-dotenv`: For loading environment variables from a `.env` file.
- `psycopg2-binary`: For connecting, querying and modifying the PostgreSQL database.

To install these dependencies, use the following command:

```zsh
pip3 install -r requirements.txt
```

## Files Explained 🗂️
- **`clean.py`**: this Python script cleans old files from an S3 bucket by checking the last modified date and deleting files older than a defined retention period (7 days). It uses AWS Lambda to perform the task and logs the results.
- **`Dockerfile`**: this Docker file creates an image for an AWS Lambda function with the necessary dependencies installed.
- **`requirements.txt`**: this project requires specific Python libraries to run correctly. These dependencies are listed in this file and are needed to ensure your environment matches the project's environment requirements.
- **`test_clean.py`**: this Python test script tests `clean.py` and its core components including the deletion of objects from an S3 bucket.

## Secrets Management 🕵🏽‍♂️
Before running the script, you need to set up your AWS credentials. Create a new file called `.env` in the `clean` directory and add the following lines, with your actual AWS keys and database details:

| Variable         | Description                                      |
|------------------|--------------------------------------------------|
| ACCESS_KEY_ID          | 	The AWS access key ID for authenticating API requests.    |
| SECRET_ACCESS_KEY          | The AWS secret access key associated with the access key ID.  |
| S3_BUCKET_NAME      | The name of the S3 bucket where the files are stored.          |
| S3_OBJECT_PREFIX          | 	The prefix used enter sub-directories in the main S3 bucket.                 |
