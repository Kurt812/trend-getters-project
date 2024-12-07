# Upload

This directory contains the necessary code to produce a Docker image to extract, format and upload the data from the BlueSky Firehose to an S3 bucket.

## Requirements 📋

To run this script, you will need the following:
- `python-dotenv`: For loading environment variables from a `.env` file.
- `psycopg2-binary`: For connecting, querying and modifying the PostgreSQL database.
- `atproto`: For connecting to the BlueSky firehose to consume and parse real-time data streams.
- `certifi`: For providing valid SSL certificates for secure connection to firehose.
- `boto3`: For integration with AWS services, including uploading files and managing data in S3 buckets.
- `freezegun`: For freezing time during testing processes.


To install these dependencies, use the following command:

```zsh
pip3 install -r requirements.txt
```


## Files Explained 🗂️
- **`dockerfile`**: this docker file creates an image with the necessary dependencies for the `upload.py` script.
- **`requirements.txt`**: this project requires specific Python libraries to run correctly. These dependencies are listed in this file and are needed to ensure your environment matches the project's environment requirements.
- **`upload.py`**: this Python script connects to the BlueSky firehose, extracts data from incoming posts, processes and formats the text, and uploads it to an S3 bucket. It handles incoming messages, extracts relevant content, and uploads them as text files with unique timestamps to the specified S3 bucket.
- **`test_upload`**: this Python test script tests key functionalities of the `upload.py` script including the uploading of data to an S3 bucket and the successful connection to the firehose.

## Secrets Management 🕵🏽‍♂️
Before running the script, you need to set up your AWS credentials. Create a new file called `.env` in the `clean` directory and add the following lines, with your actual AWS keys and database details:

| Variable         | Description                                      |
|------------------|--------------------------------------------------|
| ACCESS_KEY_ID          | 	The AWS access key ID for authenticating API requests.    |
| SECRET_ACCESS_KEY          | The AWS secret access key associated with the access key ID.  |
| S3_BUCKET_NAME      | The name of the S3 bucket where the files are stored.          |
| S3_OBJECT_PREFIX          | 	The prefix used enter sub-directories in the main S3 bucket.                 |