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

SCHEMA_NAME = ENV["SCHEMA_NAME"]
QUERY_LIST = [
    f"SELECT u.user_id, u.first_name, u.last_name, u.phone_number FROM {
        SCHEMA_NAME}.user u ORDER BY u.user_id ASC
    ",
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
    f"SELECT k.keywords_id, k.keyword FROM {
        SCHEMA_NAME}.keywords k ORDER BY k.keywords_id ASC
    ",
    f"SELECT rt.related_term_id, rt.related_term FROM {
        SCHEMA_NAME}.related_terms rt ORDER BY rt.related_term_id ASC
    ",
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
    return boto3.client(
        "s3",
        aws_access_key_id=ENV["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=ENV["AWS_SECRET_ACCESS_KEY"]
    )


def download_csv_from_s3(bucket_name: str, file_name: str) -> pd.DataFrame:
    """Downloads the current archive csv from S3"""
    s3 = s3_connection()
    try:
        s3.download_file(bucket_name, file_name, file_name)
        logging.info("Downloaded %s from S3.", file_name)
        return pd.read_csv(file_name)
    except FileNotFoundError as fe:
        logging.warning("File %s not found locally: %s", file_name, fe)
        return pd.DataFrame()
    except ClientError as ce:
        logging.warning(
            "Failed to download %s from S3 (AWS Client Error): %s", file_name, ce)
        return pd.DataFrame()
    except pd.errors.EmptyDataError as ede:
        logging.warning("Downloaded file %s is empty: %s", file_name, ede)
        return pd.DataFrame()
    except Exception as e:
        logging.error(
            "Unexpected error while downloading %s from S3: %s", file_name, e)
        raise


def upload_to_s3(bucket_name: str, file_name: str, object_name: str) -> None:
    """Uploads the updated archive files to the S3 bucket"""
    s3 = s3_connection()
    try:
        s3.upload_file(file_name, bucket_name, object_name)
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
    """Deletes the files locally"""
    try:
        if os.path.exists(file_name):
            os.remove(file_name)
            logging.info("Deleted local file: %s", file_name)
        else:
            logging.warning("File %s does not exist.", file_name)
    except PermissionError as pe:
        logging.error(
            "Permission denied while trying to delete %s: %s", file_name, pe)
        raise
    except Exception as e:
        logging.error("Unexpected error while deleting %s: %s", file_name, e)
        raise


def fetch_subscription_data_from_rds(query: str, file_name: str, bucket_name: str, folder_name: str) -> None:
    """Fetches the latest data from RDS"""
    conn = setup_connection()
    try:
        new_data = pd.read_sql(query, conn)
    except Exception as e:
        logging.error("Error while executing query: %s", e)
        raise
    finally:
        conn.close()

    s3_file_path = f"{folder_name}/{file_name}"
    existing_data = download_csv_from_s3(bucket_name, s3_file_path)

    combined_data = pd.concat([existing_data, new_data]).drop_duplicates(
    ) if not existing_data.empty else new_data
    combined_data.to_csv(file_name, index=False)

    upload_to_s3(bucket_name, file_name, s3_file_path)
    delete_local_file(file_name)


def main():
    """The main function that joins all the script functions"""
    load_dotenv()
    bucketname = ENV["S3_BUCKET_NAME"]
    foldername = "long_term_keyword_data"

    for index, query in enumerate(QUERY_LIST):
        filename = [
            "user.csv", "subscriptions.csv", "keywords.csv",
            "related_terms.csv", "terms_assignment.csv", "keyword_recording.csv"
        ][index]

        try:
            fetch_subscription_data_from_rds(
                query, filename, bucketname, foldername)
        except Exception as e:
            logging.error("Error processing %s: %s", filename, e)


if __name__ == "__main__":
    main()
