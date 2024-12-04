"""Extracts relevant data from S3 Bucket"""

import os
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from boto3 import client
from botocore.config import Config
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
    s3 = client(
        "s3",
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
        config=config
    )
    return s3

def initialize_trend_request():
    """Initialize and return a TrendReq object."""
    return TrendReq()

def fetch_file_content(s3: client, file_name: str, topic: list[str]) -> dict:
    """Checks files from S3 fir keywords and returns relevant data if keyword is found"""
    try:
        file_obj = s3.get_object(Bucket=os.environ.get("S3_BUCKET_NAME"), Key=file_name)
        file_content = file_obj['Body'].read().decode('utf-8')
        for keyword in topic:
            if keyword in file_content:
                logging.info("Keyword '%s' found in %s", keyword, file_name)
                return {
                    "Text": file_content,
                    "Keyword": keyword,
                }
    except FileNotFoundError as e:
        logging.error("No files found in S3: %s", e)
    return None

def access_bluesky_files(s3: client, topic: list[str]) -> list[str]:
    """Accesses files from S3 and returns a list of texts with the topic present"""
    continuation_token = None
    file_names = []

    #continously fetches .txt files from bucket until all files have been fetched
    while True:
        bucket_parameters = {'Bucket': os.environ.get("S3_BUCKET_NAME")}
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

    results = access_bluesky_files(s3, topic)
    logging.info(type(results))

    return pd.DataFrame(results)

def fetch_suggestions(pytrend: TrendReq, keyword: str):
    """Fetch and print suggestions for a given keyword."""
    return pytrend.suggestions(keyword=keyword)

def main(topic: list[str]) -> pd.DataFrame:
    """Extracts data from S3 Bucket and google trends"""
    extract_dataframe = create_dataframe(topic)

    pytrend = initialize_trend_request()
    extract_dataframe['related_terms'] = ""

    for keyword in topic:
        extract_dataframe.loc[extract_dataframe['Keyword']
                              == keyword, 'related_terms'] = ",".join(
            [suggestion['title'] for suggestion in fetch_suggestions(pytrend, keyword)]
        )
    return extract_dataframe

if __name__ == "__main__":
    topics = ['wine','river']
    extracted_dataframe = main(topics)

    print(extracted_dataframe)
