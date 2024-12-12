"""Combine keyword_recordings in S3 and RDS"""
from os import environ as ENV
import logging
import boto3
import pandas as pd
import os
import psycopg2
from dotenv import load_dotenv


def get_connection():
    """Function to connect to RDS."""
    return psycopg2.connect(
        host=ENV["DB_HOST"],
        port=ENV["DB_PORT"],
        database=ENV["DB_NAME"],
        user=ENV["DB_USERNAME"],
        password=ENV["DB_PASSWORD"]
    )


def download_csv_from_s3_to_dataframe(bucket_name, folder_name, file_name) -> pd.DataFrame:
    """Downloads a CSV file from a specific folder in an S3 bucket and loads it into a Pandas DataFrame"""
    s3_client = boto3.client('s3')

    object_key = f"{folder_name}/{file_name}"

    temp_file_path = os.path.join(os.getcwd(), file_name)

    try:
        s3_client.download_file(bucket_name, object_key, temp_file_path)
        logging.info("File downloaded successfully: %s", temp_file_path)

        df = pd.read_csv(temp_file_path)
        logging.info("File loaded into Pandas DataFrame successfully.")

        os.remove(temp_file_path)
        logging.info("Temporary file removed.")
        df['date_and_hour'] = pd.to_datetime(df['date_and_hour'])
        return df
    except Exception as e:
        logging.error("An error occurred: %s", e)
        return None


def fetch_keyword_recordings_as_dataframe():

    try:
        connection = get_connection()

        query = f"""
        SET search_path TO {ENV["SCHEMA_NAME"]};
        SELECT keyword_recordings_id, keywords_id, total_mentions, avg_sentiment, date_and_hour
        FROM keyword_recordings;
        """

        dataframe = pd.read_sql_query(query, connection)
        return dataframe

    except psycopg2.Error as e:
        logging.error("Database error: %s", e)
        return None
    finally:
        if connection:
            connection.close()


def main_combine() -> pd.DataFrame:
    """Main function to produce dataframe from S3 bucket."""
    load_dotenv()

    BUCKET_NAME = ENV["S3_BUCKET_NAME"]
    FOLDER_NAME = ENV["S3_FOLDER_NAME"]
    FILE_NAME = ENV["S3_FILE_NAME"]

    s3_df = download_csv_from_s3_to_dataframe(
        BUCKET_NAME, FOLDER_NAME, FILE_NAME)
    db_df = fetch_keyword_recordings_as_dataframe()

    if s3_df is not None and db_df is not None:
        combined_df = pd.concat([s3_df, db_df], ignore_index=True)
        logging.info("Combined DataFrame from S3 created successfully.")
        return combined_df
    else:
        logging.ERROR("Could not combine DataFrames due to missing data.")
        return None
