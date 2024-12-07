"""This script updates the archive data files in the S3 bucket"""
from sqlalchemy import create_engine
import os
import logging
from os import environ as ENV
import pandas as pd
import psycopg2
import psycopg2.extras
from psycopg2 import OperationalError, InterfaceError, DatabaseError
from dotenv import load_dotenv
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError
from sqlalchemy.exc import SQLAlchemyError

load_dotenv()

SCHEMA_NAME = ENV["SCHEMA_NAME"]
UPDATE_QUERY = f"""
        SELECT
            *
        FROM
            {SCHEMA_NAME}.keyword_recordings kr
        WHERE created_at < NOW() - INTERVAL '24 HOURS'
        """
REMOVE_QUERY = f"""
        DELETE FROM {SCHEMA_NAME}.keyword_recordings
        WHERE created_at < NOW() - INTERVAL '24 HOURS'
    """

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler()
    ]
)


def setup_engine():
    """Set up SQLAlchemy engine."""
    try:
        db_username = ENV['DB_USERNAME']
        db_password = ENV['DB_PASSWORD']
        db_host = ENV['DB_HOST']
        db_port = ENV['DB_PORT']
        db_name = ENV['DB_NAME']

        if not all([db_username, db_password, db_host, db_port, db_name]):
            raise EnvironmentError(
                "One or more required environment variables are missing.")

        engine = create_engine(
            f"postgresql+psycopg2://{db_username}:{db_password}@{
                db_host}:{db_port}/{db_name}"
        )
        return engine
    except SQLAlchemyError as e:
        logging.error("Failed to connect to database: %s", e)
        raise
    except Exception as e:
        logging.error("An unexpected error occurred: %s", e)
        raise


def setup_connection() -> tuple:
    """Sets up a connection to the RDS"""
    try:
        conn = psycopg2.connect(
            user=ENV["DB_USERNAME"],
            password=ENV["DB_PASSWORD"],
            host=ENV["DB_HOST"],
            port=ENV["DB_PORT"],
            database=ENV["DB_NAME"]
        )
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        logging.info('Connection successfully established to RDS database.')
        return conn, cursor
    except OperationalError as oe:
        logging.error(
            "Operational error while connecting to the database: %s", oe)
        raise
    except InterfaceError as ie:
        logging.error(
            "Interface error while connecting to the database: %s", ie)
        raise
    except DatabaseError as de:
        logging.error("Database error occurred: %s", de)
        raise
    except Exception as e:
        logging.error(
            "Unexpected error while setting up the database connection: %s", e)
        raise


def s3_connection() -> boto3.client:
    """Function connects to S3 and provides client to interact with it."""
    try:
        aws_access_key_id = os.environ.get("ACCESS_KEY_ID")
        aws_secret_access_key = os.environ.get("SECRET_ACCESS_KEY")
        if not aws_access_key_id or not aws_secret_access_key:
            logging.error("Missing required AWS credentials in .env file.")
            raise ValueError("Missing AWS credentials.")

        s3 = boto3.client("s3", aws_access_key_id,
                          aws_secret_access_key)
        return s3
    except (NoCredentialsError, PartialCredentialsError) as e:
        logging.error("A BotoCore error occurred: %s", e)
        raise
    except Exception as e:
        logging.error(
            "An unexpected error occurred while connecting to S3: %s", e)
        raise


def download_csv_from_s3(bucket_name: str, s3_file_path: str, file_name: str) -> pd.DataFrame:
    """Downloads the files from the s3 bucket"""
    tmp_file_name = f"/tmp/{file_name}"
    s3 = s3_connection()
    try:
        s3.download_file(bucket_name, s3_file_path, tmp_file_name)
        logging.info("Downloaded %s from S3.", tmp_file_name)
        return pd.read_csv(tmp_file_name)
    except FileNotFoundError as fe:
        logging.warning("File %s not found locally: %s", tmp_file_name, fe)
        return pd.DataFrame()
    except pd.errors.EmptyDataError as ede:
        logging.warning("Downloaded file %s is empty: %s", tmp_file_name, ede)
        return pd.DataFrame()
    except ClientError as ce:
        logging.error(
            "Failed to download %s from S3 (AWS Client Error): %s", tmp_file_name, ce)
        raise
    except Exception as e:
        logging.error(
            "Unexpected error while downloading %s from S3: %s", tmp_file_name, e)
        raise


def upload_to_s3(bucket_name: str, file_name: str, object_name: str) -> None:
    """Uploads the updated archive files to the S3 bucket"""
    s3 = s3_connection()
    try:
        tmp_file_name = f"/tmp/{file_name}"
        s3.upload_file(tmp_file_name, bucket_name, object_name)
        logging.info("Uploaded %s to s3://%s/%s",
                     file_name, bucket_name, object_name)
    except FileNotFoundError as fe:
        logging.error("Local file %s not found for upload: %s", file_name, fe)
        raise
    except NoCredentialsError as nce:
        logging.error("AWS credentials not found: %s", nce)
        raise
    except ClientError as ce:
        logging.error(
            "Failed to upload %s to S3 (AWS Client Error): %s", file_name, ce)
        raise
    except Exception as e:
        logging.error(
            "Unexpected error while uploading %s to S3: %s", file_name, e)
        raise


def delete_local_file(file_name: str) -> None:
    """Deletes the files from the lambda to keep it tidy"""
    tmp_file_name = f"/tmp/{file_name}"
    try:
        if os.path.exists(tmp_file_name):
            os.remove(tmp_file_name)
            logging.info("Deleted local file: %s", tmp_file_name)
        else:
            logging.warning("File %s does not exist.", tmp_file_name)
    except PermissionError as pe:
        logging.error(
            "Permission denied while trying to delete %s: %s", tmp_file_name, pe)
        raise
    except Exception as e:
        logging.error("Unexpected error while deleting %s: %s",
                      tmp_file_name, e)
        raise


def fetch_subscription_data_from_rds(query: str, file_name: str, bucket_name: str, folder_name: str) -> None:
    """Fetches the latest data from RDS"""
    engine = setup_engine()
    try:
        new_data = pd.read_sql(query, engine)
    except Exception as e:
        logging.error("Error while executing query: %s", e)
        raise

    s3_file_path = f"{folder_name}/{file_name}"
    existing_data = download_csv_from_s3(bucket_name, s3_file_path, file_name)

    combined_data = pd.concat([existing_data, new_data]).drop_duplicates(
    ) if not existing_data.empty else new_data
    tmp_file_name = f"/tmp/{file_name}"

    combined_data.to_csv(tmp_file_name, index=False)

    upload_to_s3(bucket_name, file_name, s3_file_path)
    delete_local_file(file_name)


def clear_keyword_recordings() -> None:
    """Clears data older than 24 hours from keywords recording table"""
    conn, cursor = setup_connection()
    try:
        cursor.execute(REMOVE_QUERY)
        conn.commit()
    except Exception as e:
        logging.error("Error while executing query: %s", e)
        raise
    finally:
        conn.close()


def lambda_handler(event, context):
    """The main function that joins all the script functions"""
    load_dotenv()
    bucketname = ENV["S3_BUCKET_NAME"]
    foldername = "long_term_keyword_data"
    filename = "keyword_recording.csv"

    try:
        fetch_subscription_data_from_rds(
            UPDATE_QUERY, filename, bucketname, foldername)
        clear_keyword_recordings()
    except Exception as e:
        logging.error("Error processing %s: %s", filename, e)


if __name__ == "__main__":
    lambda_handler(None, None)
