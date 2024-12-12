# Dashboard

This folder contains all the code, documentation and resources necessary for the Dashboard to run successfully.

## Diagram 📊

### Dashboard Wireframe

![Dashboard Wireframe](/images/dashboard_wireframe.png)

## Requirements 📋

To run this script and containerize into a docker image, you will need the following:
- `python-dotenv`: For loading environment variables from a `.env` file.
- `streamlit`: For creating an interactive web application for visualisations/
- `altair`: For creating declarative statistical visualisations.

To install these dependencies, use the following command:

```zsh
pip3 install -r requirements.txt
```

## Files Explained 🗂️
- **`dashboard.py`**: this streamlit python file creates an application that allows users to track and submit trending topics by verifying their details. 
- **`combined_data.py`**: this Python script combines keyword recording data from an S3 bucket and an RDS database into a single Pandas DataFrame, while handling errors gracefully. The script is designed for seamless integration of keyword recording data for further processing or analysis.
- **`predict_mentions.py`**: this Python script predicts the total mentions for the next hour for a given keyword. Using a RandomForestRegressor model, the script trains and scales the data to make predictions based on recent trends, providing actionable insights for future keyword activity.
- **`queries.py`**: this Python script provides utility functions for querying a PostgreSQL database to retrieve insights for a dashboard.
- **`requirements.txt`**: this project requires specific Python libraries to run correctly. These dependencies are listed in this file and are needed to ensure your environment matches the project's environment requirements.

## Secrets Management 🕵🏽‍♂️

Before running the script, you need to set up your AWS credentials. Create a new file called `.env` in the `dashboard` directory and add the following lines, with your actual AWS keys and database details:

| Variable         | Description                                      |
|------------------|--------------------------------------------------|
| API_ENDPOINT     | The API endpoint for the submission of topics.   |
| DB_HOST          | The hostname or IP address of the database.      |
| DB_PORT          | The port number for the database connection.     |
| DB_PASSWORD      | The password for the database user.              |
| DB_USERNAME      | The username for the database.                   |
| DB_NAME          | The name of the database.                        |
| SCHEMA_NAME      | The name of the database schema.                 |
| VPC_ID           | The identifier for the Virtual Private Cloud (VPC) associated with the database. |





