# Pipeline

This directory contains all the necessary code and documentation for the Extract Transform Load (ETL) pipeline used to collect, clean and upload the data collected from the live data stream(s).

## Requirements 📋

To run this script, you will need the following:
- `pytest`: For running unit tests.
- `pytest-cov`: For measuring test coverage.
- `requests`: For making HTTP requests.
- `pandas`: For data manipulation and analysis.
- `python-dotenv`: For loading environment variables from a `.env` file.
- `psycopg2-binary`: For connecting, querying and modifying the PostgreSQL database.
- `vaderSentiment`: For performing sentiment analysis on the text data.
- `atproto`: For connecting to the BlueSky firehose to consume and parse real-time data streams.
- `certifi`: For providing valid SSL certificates for secure connection to firehose.
- `freezegun`: For freezing time during testing processes.
- `boto3`: For integration with AWS services, including uploading files and managing data in S3 buckets.
- `flask`: For creating RESTful APIs to interact with and manage data.


To install these dependencies, use the following command:

```zsh
pip3 install -r requirements.txt
```

## Files Explained 🗂️
- **`Dockerfile`**: this docker file creates an image along with the required dependencies and files for the `api.py` file that can be accessed vi*a port 5000.
- **`api.py`**: this python script creates a Flask web application with an 'POST' endpoint for the creation of new topics.
- **`bluesky_extract.py`**: this python script connects to the BlueSky Firehose and extracts data relevant to user-defined topics. This raw data is then uploaded to an S3 bucket for further processing and use.
- **`connect.sh`**: this is a bash script written to establish a connection with the PostgreSQL database using environment variables loaded from a `.env` file.
- **`load.py`**: this Python script uploads topic data into an RDS database by inserting entries into a specified schema and table.
- **`requirements.txt`**: this project requires specific Python libraries to run correctly. These dependencies are listed in this file and are needed to ensure your environment matches the project's environment requirements.
- **`reset.sh`**: this is a bash utilises script environment variables to reset the the PostgreSQL database by dropping existing tables if they exist and recreating the,.
- **`schema.sql`**: this SQL file that defines the database schema and creates the necessary tables. It also seeds the data_source table with predefined known data sources and defines relationships between tables.
- **`test_bluesky_extract.py`**: this is a python test script that looks to test key functionalities of the `bluesky_extract.py` such as the connection to the BlueSky firehose and the successful identification of topic words in posts.
- **`transform.py`**: this Python script retrieves raw data, removes duplicates, assigns keyword IDs, computes sentiment scores using VADER, and outputs a processed DataFrame.

## Secrets Management 🕵🏽‍♂️
Before running the script, you need to set up your AWS credentials. Create a new file called `.env` in the `pipeline` directory and add the following lines, with your actual AWS keys and database details:

| Variable         | Description                                      |
|------------------|--------------------------------------------------|
| DB_HOST          | The hostname or IP address of the database.      |
| DB_PORT          | The port number for the database connection.     |
| DB_PASSWORD      | The password for the database user.              |
| DB_USERNAME          | The username for the database.                   |
| DB_NAME          | The name of the database.                        |
| SCHEMA_NAME      | The name of the database schema.                 |
