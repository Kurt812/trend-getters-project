# Dashboard

This folder contains all the code, documentation and resources necessary for the Dashboard to run successfully.

## Diagram 📊

### Dashboard Wireframe

![Dashboard Wireframe]()

## Requirements 📋

To run this script and containerize into a docker image, you will need the following:

- **Python**: Version 3.10
- `pytest`: For running unit tests
- `pytest-cov`: For measuring test coverage
- `pandas`: For data manipulation and analysis
- `python-dotenv`: For loading environment variables from a `.env` file
- `streamlit`: For creating an interactive web application for visualisations
- `altair`: For creating declarative statistical visualisations

To install these dependencies, use the following command:

```zsh
pip3 install -r requirements.txt
```

## Files Explained 🗂️


## Secrets Management 🕵🏽‍♂️

Before running the script, you need to set up your AWS credentials. Create a new file called `.env` in the `dashboard` directory and add the following lines, with your actual AWS keys and database details:

| Variable         | Description                                      |
|------------------|--------------------------------------------------|
| DB_HOST          | The hostname or IP address of the database.      |
| DB_PORT          | The port number for the database connection.     |
| DB_PASSWORD      | The password for the database user.              |
| DB_USER          | The username for the database.                   |
| DB_NAME          | The name of the database.                        |
| SCHEMA_NAME      | The name of the database schema.                 |





