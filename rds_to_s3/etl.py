from os import environ as ENV
import pandas as pd
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
import boto3
from botocore.exceptions import NoCredentialsError
from datetime import datetime


def setup_connection():
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


def create_query_list() -> list[str]:
    """Puts all the queries into a list"""
    SCHEMA_NAME = ENV["SCHEMA_NAME"]
    user_subscription_query = f"""
        SELECT
            u.user_id,
            u.first_name,
            u.last_name,
            u.phone_number,
            s.subscription_id,
            s.subscription_status,
            s.notification_threshold,
            s.keywords_id
        FROM
            {SCHEMA_NAME}.subscription s
        JOIN
            {SCHEMA_NAME}.user u
        ON
            s.user_id = u.user_id
        ORDER BY
            u.user_id ASC;
    """

    keyword_query = f"""
        SELECT
            k.keywords_id,
            k.keyword
        FROM
            {SCHEMA_NAME}.keywords k
        ORDER BY
            k.keywords_id ASC;
    """

    related_terms_query = f"""
        SELECT
            rt.related_term_id,
            rt.related_term
        FROM
            {SCHEMA_NAME}.related_terms rt
        ORDER BY
            rt.related_term_id ASC;
    """

    term_assignment_query = f"""
        SELECT
            rta.related_term_assignment,
            rta.keywords_id,
            rta.related_term_id
        FROM
            {SCHEMA_NAME}.related_term_assignment rta
        ORDER BY
            rta.related_term_assignment ASC;
    """

    keyword_recordings_query = f"""
        SELECT
            kr.keyword_recordings_id,
            kr.keywords_id,
            kr.total_mentions,
            kr.hour_of_day,
            kr.avg_sentiment
        FROM
            {SCHEMA_NAME}.keyword_recordings kr
        ORDER BY
            kr.keyword_recordings_id ASC;
    """

    query_list = [user_subscription_query, keyword_query,
                  related_terms_query, term_assignment_query, keyword_recordings_query]
    return query_list


def upload_to_s3(bucket_name: str, file_name: str, object_name: str):
    """
    Upload a file to an S3 bucket.

    :param bucket_name: S3 bucket name
    :param file_name: Local file path
    :param object_name: S3 object name (key)
    """
    s3 = boto3.client(
        "s3",
        aws_access_key_id=ENV["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=ENV["AWS_SECRET_ACCESS_KEY"]
    )
    try:
        s3.upload_file(file_name, bucket_name, object_name)
        print(f"Uploaded {file_name} to s3://{bucket_name}/{object_name}")
    except FileNotFoundError:
        print(f"File {file_name} not found.")
    except NoCredentialsError:
        print("AWS credentials not available.")
    except Exception as e:
        print(f"Failed to upload {file_name} to S3: {e}")


def fetch_subscription_data_from_rds(query: str, filename: str, bucket_name: str, folder_name: str) -> pd.DataFrame:
    """Fetch the subscription data from the database and upload to S3."""
    conn = setup_connection()
    if not conn:
        return pd.DataFrame()

    dataframe = pd.read_sql(query, conn)
    conn.close()

    # Save the DataFrame to a CSV
    dataframe.to_csv(filename, index=False)

    # Dynamically create the S3 folder path
    s3_key = f"{folder_name}/{filename}"

    # Upload to S3
    upload_to_s3(bucket_name, filename, s3_key)

    return dataframe


if __name__ == "__main__":
    load_dotenv()
    query_list = create_query_list()
    # Define the S3 bucket name in your .env file
    bucket_name = ENV["S3_BUCKET_NAME"]

    # Generate a unique folder name using the current timestamp
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    folder_name = f"data_export/{timestamp}"

    for index, query in enumerate(query_list):
        if index == 0:
            filename = "user_subscription.csv"
        elif index == 1:
            filename = "keywords.csv"
        elif index == 2:
            filename = "related_terms.csv"
        elif index == 3:
            filename = "terms_assignment.csv"
        elif index == 4:
            filename = "keyword_recordings.csv"

        fetch_subscription_data_from_rds(
            query, filename, bucket_name, folder_name)
