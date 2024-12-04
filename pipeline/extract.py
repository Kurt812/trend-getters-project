"""Extracts relevant data from S3 Bucket"""

import os
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from boto3 import client
from botocore.config import Config
from botocore.exceptions import ClientError
import pandas as pd
from dotenv import load_dotenv
from pytrends.request import TrendReq

load_dotenv(".env")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
    ]
)


def s3_connection() -> client:
    """Connects to an S3"""
    config = Config(
        connect_timeout=5,
        read_timeout=10,
        retries={
            'max_attempts': 3,
            'mode': 'standard'
        },
        max_pool_connections=50
    )
    try:

        aws_access_key = os.environ.get("AWS_ACCESS_KEY_ID")
        aws_secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")

        if not aws_access_key or not aws_secret_key:
            logging.error("Missing required AWS credentials in .env file.")
            raise
        s3 = client(
            "s3",
            aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.environ.get(
                "AWS_SECRET_ACCESS_KEY"),
            config=config
        )
    except Exception as e:
        logging.error('An error occurred attempting to connect to S3: ', e)
        return None
    return s3


def initialize_trend_request() -> TrendReq:
    """Initialize and return a TrendReq object."""
    return TrendReq()


def fetch_file_content(s3: client, file_name: str, topic: list[str]) -> dict:
    """Checks files from S3 for keywords and returns relevant data if keyword is found"""
    try:
        file_obj = s3.get_object(Bucket=os.environ.get(
            "S3_BUCKET_NAME"), Key=file_name)
        file_content = file_obj['Body'].read().decode('utf-8')
        for keyword in topic:
            if keyword in file_content:
                logging.info("Keyword '%s' found in %s", keyword, file_name)
                return {
                    "Text": file_content,
                    "Keyword": keyword,
                }
        logging.error("No keyword found in file %s", file_name)
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            logging.error("No files found in S3: %s", e)
            raise FileNotFoundError(f"File '{file_name}' not found in S3.")

    return None


def extract_bluesky_files(s3: client, topic: list[str]) -> list[str]:
    """Accesses files from S3 and returns a list of texts with the topic present"""
    continuation_token = None
    file_names = []

    bucket_parameters = {'Bucket': os.environ.get("S3_BUCKET_NAME")}
    if not bucket_parameters.get('Bucket'):
        raise KeyError("Missing environment variable: %s",
                       bucket_parameters.keys())

    # continously fetches .txt files from bucket until all files have been fetched
    while True:
        if continuation_token:
            bucket_parameters['ContinuationToken'] = continuation_token

        response = s3.list_objects_v2(**bucket_parameters)
        contents = response.get("Contents")

        for file in contents:
            if file['Key'].endswith(".txt"):
                file_names.append(file['Key'])

        continuation_token = response.get('NextContinuationToken')
        if not continuation_token:
            break
    return file_names


def multi_threading_matching(s3: client, topic: list[str], file_names: list[str],) -> list[str]:
    """Uses multi-threading to extract matching text from S3 files"""
    matching_texts = []
    with ThreadPoolExecutor(max_workers=40) as thread_pool:
        submitted_tasks = [thread_pool.submit(fetch_file_content, s3,
                                              file_name, topic) for file_name in file_names]
        for completed_task in as_completed(submitted_tasks):
            matching_result = completed_task.result()
            if matching_result:
                matching_texts.append(matching_result)

    return matching_texts


def create_dataframe(topic: list[str]) -> pd.DataFrame:
    """Main function to extract data and return a DataFrame of the relevant data"""
    s3 = s3_connection()

    filenames = extract_bluesky_files(s3, topic)
    matching_file_texts = multi_threading_matching(s3, topic, filenames)
    if matching_file_texts:
        return pd.DataFrame(matching_file_texts)
    raise ValueError("No matches found.")


def fetch_suggestions(pytrend: TrendReq, keyword: str) -> list[dict]:
    """Fetch and print suggestions for a given keyword."""
    return pytrend.suggestions(keyword=keyword)


def main(topic: list[str]) -> pd.DataFrame:
    """Extracts data from S3 Bucket and google trends"""
    extract_dataframe = create_dataframe(topic)
    pytrend = initialize_trend_request()
    extract_dataframe['Related Terms'] = ""

    for keyword in topic:
        extract_dataframe.loc[extract_dataframe['Keyword']
                              == keyword, 'Related Terms'] = ",".join(
            [suggestion['title']
                for suggestion in fetch_suggestions(pytrend, keyword)]
        )
    return extract_dataframe


if __name__ == "__main__":
    topics = ['good', 'river']
    extracted_dataframe = main(topics)

    logging.info("\n %s", extracted_dataframe)
