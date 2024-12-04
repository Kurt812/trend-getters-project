# Pipeline

This directory contains all the necessary code and documentation for the Extract Transform Load (ETL) pipeline used to collect, clean and upload the data collected from the live data stream(s).

## Requirements 📋

To run this script, you will need the following:
- **Python**: Version 3.10
- `pytest`: For running unit tests
- `pytest-cov`: For measuring test coverage
- `requests`: For making HTTP requests
- `pandas`: For data manipulation and analysis
- `python-dotenv`: For loading environment variables from a `.env` file


To install these dependencies, use the following command:

```zsh
pip3 install -r requirements.txt
```

## Files Explained 🗂️
- `Dockerfile`: this docker file creates an image along with the required dependencies and files for the `api.py` file that can be accessed via port 5000.
- `api.py
- `connect.sh`: this is a bash script written to establish a connection with the PostgreSQL database using environment variables loaded from a .env file.
- `reset.sh`: this is a bash utilises script environment variables to reset the the PostgreSQL database by dropping existing tables if they exist and recreating the,.
- `schema.sql`: this SQL file that defines the database schema and creates the necessary tables. It also seeds the data_source table with predefined known data sources and defines relationships between tables.

## Secrets Management 🕵🏽‍♂️
Before running the script, you need to set up your AWS credentials. Create a new file called `.env` in the `pipeline` directory and add the following lines, with your actual AWS keys and database details:

| Variable         | Description                                      |
|------------------|--------------------------------------------------|
| DB_HOST          | The hostname or IP address of the database.      |
| DB_PORT          | The port number for the database connection.     |
| DB_PASSWORD      | The password for the database user.              |
| DB_USER          | The username for the database.                   |
| DB_NAME          | The name of the database.                        |
| SCHEMA_NAME      | The name of the database schema.                 |
