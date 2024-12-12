# Notifications

This folder contains all the code, documentation and resources necessary for the notifications to run successfully.

## Requirements 📋

To run this script and containerize into a docker image, you will need the following:
- `boto3`: For integration with AWS services, including uploading files and managing data in S3 buckets.
- `psycopg2-binary`: For connecting, querying and modifying the PostgreSQL database.
- `python-dotenv`: For loading environment variables from a `.env` file.

To install these dependencies, use the following command:

```zsh
pip3 install -r requirements.txt
```

## Files Explained 🗂️
- **`Dockerfile`**: this docker file creates an image along with the required dependencies and files for the `notify.py` file.
- **`notify.py`**: this Python script sends personalized email notifications to users subscribed to keyword trends. It connects to a PostgreSQL database to fetch significant changes in keyword mentions and matches them with user subscription thresholds. 
- **`requirements.txt`**: this project requires specific Python libraries to run correctly. These dependencies are listed in this file and are needed to ensure your environment matches the project's environment requirements.

## Secrets Management 🕵🏽‍♂️

Before running the script, you need to set up your AWS credentials. Create a new file called `.env` in the `dashboard` directory and add the following lines, with your actual AWS keys and database details:

| Variable         | Description                                      |
|------------------|--------------------------------------------------|
| DB_HOST          | The hostname or IP address of the database.      |
| DB_PORT          | The port number for the database connection.     |
| DB_PASSWORD      | The password for the database user.              |
| DB_USERNAME      | The username for the database.                   |
| DB_NAME          | The name of the database.                        |
| SCHEMA_NAME      | The name of the database schema.                 |






