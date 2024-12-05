"""This script updates the archive data files in the S3 bucket"""

import os
import logging
from os import environ as ENV
import pandas as pd
import psycopg2
import psycopg2.extras
from psycopg2 import OperationalError, InterfaceError, DatabaseError
from dotenv import load_dotenv
import boto3
from botocore.exceptions import NoCredentialsError, ClientError
from datetime import datetime


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("app.log"),
        logging.StreamHandler()
    ]
)


def setup_connection() -> None:
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
        cursor.execute(f"SET SEARCH_PATH TO {ENV['SCHEMA_NAME']};")
        return conn
    except OperationalError as oe:
        logging.error(
            f"Operational error while connecting to the database: {oe}")
        raise
    except InterfaceError as ie:
        logging.error(
            f"Interface error while connecting to the database: {ie}")
        raise
    except DatabaseError as de:
        logging.error(f"Database error occurred: {de}")
        raise
    except Exception as e:
        logging.error(
            f"Unexpected error while setting up the database connection: {e}")
        raise


def create_query_list() -> list[str]:
    """Makes a list of the queries to extract data from the RDS"""
    SCHEMA_NAME = ENV["SCHEMA_NAME"]
    return [
        f"SELECT u.user_id, u.first_name, u.last_name, u.phone_number FROM {SCHEMA_NAME}.user u ORDER BY u.user_id ASC;",
        f"""
        SELECT
            s.subscription_id, s.user_id, s.subscription_status,
            s.notification_threshold, s.keywords_id
        FROM
            {SCHEMA_NAME}.subscription s
        JOIN
            {SCHEMA_NAME}.user u ON s.user_id = u.user_id
        ORDER BY u.user_id ASC;
        """,
        f"SELECT k.keywords_id, k.keyword FROM {SCHEMA_NAME}.keywords k ORDER BY k.keywords_id ASC;",
        f"SELECT rt.related_term_id, rt.related_term FROM {SCHEMA_NAME}.related_terms rt ORDER BY rt.related_term_id ASC;",
        f"""
        SELECT
            rta.related_term_assignment, rta.keywords_id, rta.related_term_id
        FROM
            {SCHEMA_NAME}.related_term_assignment rta
        ORDER BY rta.related_term_assignment ASC;
        """,
        f"""
        SELECT
            kr.keyword_recordings_id, kr.keywords_id, kr.total_mentions,
            kr.hour_of_day, kr.avg_sentiment
        FROM
            {SCHEMA_NAME}.keyword_recordings kr
        ORDER BY kr.keyword_recordings_id ASC;
        """
    ]


def download_csv_from_s3(bucket_name: str, file_name: str) -> pd.DataFrame:
    """Downloads the current archive csv from S3"""
    s3 = boto3.client(
        "s3",
        aws_access_key_id=ENV["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=ENV["AWS_SECRET_ACCESS_KEY"]
    )
    try:
        s3.download_file(bucket_name, file_name, file_name)
        logging.info(f"Downloaded {file_name} from S3.")
        return pd.read_csv(file_name)
    except FileNotFoundError as fe:
        logging.warning(f"File {file_name} not found locally: {fe}")
        return pd.DataFrame()
    except ClientError as ce:
        logging.warning(
            f"Failed to download {file_name} from S3 (AWS Client Error): {ce}")
        return pd.DataFrame()
    except pd.errors.EmptyDataError as ede:
        logging.warning(f"Downloaded file {file_name} is empty: {ede}")
        return pd.DataFrame()
    except Exception as e:
        logging.error(
            f"Unexpected error while downloading {file_name} from S3: {e}")
        raise


def upload_to_s3(bucket_name: str, file_name: str, object_name: str) -> None:
    """Uploads the updated archive files to the S3 bucket"""
    s3 = boto3.client(
        "s3",
        aws_access_key_id=ENV["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=ENV["AWS_SECRET_ACCESS_KEY"]
    )
    try:
        s3.upload_file(file_name, bucket_name, object_name)
        logging.info(
            f"Uploaded {file_name} to s3://{bucket_name}/{object_name}")
    except FileNotFoundError as fe:
        logging.error(f"Local file {file_name} not found for upload: {fe}")
        raise
    except NoCredentialsError as nce:
        logging.error(f"AWS credentials not found: {nce}")
        raise
    except ClientError as ce:
        logging.error(
            f"Failed to upload {file_name} to S3 (AWS Client Error): {ce}")
        raise
    except Exception as e:
        logging.error(
            f"Unexpected error while uploading {file_name} to S3: {e}")
        raise


def delete_local_file(file_name: str) -> None:
    try:
        if os.path.exists(file_name):
            os.remove(file_name)
            logging.info(f"Deleted local file: {file_name}")
        else:
            logging.warning(f"File {file_name} does not exist.")
    except PermissionError as pe:
        logging.error(
            f"Permission denied while trying to delete {file_name}: {pe}")
        raise
    except Exception as e:
        logging.error(f"Unexpected error while deleting {file_name}: {e}")
        raise


def fetch_subscription_data_from_rds(query: str, filename: str, bucket_name: str, folder_name: str):
    conn = setup_connection()
    try:
        new_data = pd.read_sql(query, conn)
    except Exception as e:
        logging.error(f"Error while executing query: {e}")
        raise
    finally:
        conn.close()

    s3_file_path = f"{folder_name}/{filename}"
    existing_data = download_csv_from_s3(bucket_name, s3_file_path)

    combined_data = pd.concat([existing_data, new_data]).drop_duplicates(
    ) if not existing_data.empty else new_data
    combined_data.to_csv(filename, index=False)

    upload_to_s3(bucket_name, filename, s3_file_path)
    delete_local_file(filename)

    return combined_data


if __name__ == "__main__":
    load_dotenv()
    query_list = create_query_list()
    bucket_name = ENV["S3_BUCKET_NAME"]
    folder_name = "long_term_keyword_data"

    for index, query in enumerate(query_list):
        filename = [
            "user.csv", "subscriptions.csv", "keywords.csv",
            "related_terms.csv", "terms_assignment.csv", "keyword_recording.csv"
        ][index]

        try:
            fetch_subscription_data_from_rds(
                query, filename, bucket_name, folder_name)
        except Exception as e:
            logging.error(f"Error processing {filename}: {e}")
