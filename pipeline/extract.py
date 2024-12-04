from collections import defaultdict
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
    handlers=[logging.StreamHandler()]
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
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            config=config
        )
    except Exception as e:
        logging.error('An error occurred attempting to connect to S3: %s', e)
        return None
    return s3

def extract_bluesky_files(s3: client) -> list[str]:
    """Accesses files from S3 and returns a list of texts with the topic present"""
    continuation_token = None
    file_names = []

    bucket_parameters = {'Bucket': os.environ.get("S3_BUCKET_NAME")}
    if not bucket_parameters.get('Bucket'):
        raise KeyError("Missing environment variable: S3_BUCKET_NAME")

    while True:
        if continuation_token:
            bucket_parameters['ContinuationToken'] = continuation_token

        response = s3.list_objects_v2(**bucket_parameters)
        contents = response.get("Contents")

        if not contents:
            break

        for file in contents:
            if file['Key'].endswith(".txt"):
                file_names.append(file['Key'])

        continuation_token = response.get('NextContinuationToken')
        if not continuation_token:
            break

    return file_names

def fetch_file_content(s3: client, file_name: str, topic: list[str]) -> dict:
    """Checks files from S3 for keywords and returns relevant data if keyword is found"""
    try:
        file_obj = s3.get_object(Bucket=os.environ.get("S3_BUCKET_NAME"), Key=file_name)
        file_content = file_obj['Body'].read().decode('utf-8')
        hour_folder = file_name.split('/')[-2]  # Extracts the hour folder (e.g., "16/")

        extracted_texts = []
        keyword_counts = defaultdict(int)
        for keyword in topic:
            count = file_content.count(keyword)
            if count > 0:
                logging.info("Keyword '%s' found %d times in %s", keyword, count, file_name)
                extracted_texts.append({"Text": file_content, "Keyword": keyword})
                keyword_counts[keyword] += count

        return {
            "Hour": hour_folder,
            "Extracted Texts": extracted_texts,
            "Counts": dict(keyword_counts)
        }

    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            logging.error("File not found in S3: %s", e)
            raise FileNotFoundError(f"File '{file_name}' not found in S3.") from e

    return None

def multi_threading_matching(s3: client, topic: list[str], file_names: list[str]) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Uses multi-threading to extract matching text and keyword counts from S3 files"""
    extracted_texts = []
    hourly_data = defaultdict(lambda: defaultdict(int))

    with ThreadPoolExecutor(max_workers=40) as thread_pool:
        submitted_tasks = [thread_pool.submit(fetch_file_content, s3, file_name, topic)
                           for file_name in file_names]

        for completed_task in as_completed(submitted_tasks):
            result = completed_task.result()
            if result:
                hour = result["Hour"]
                extracted_texts.extend(result["Extracted Texts"])
                for keyword, count in result["Counts"].items():
                    hourly_data[hour][keyword] += count

    original_df = pd.DataFrame(extracted_texts)

    hourly_rows = [{"Hour": hour, "Keyword": keyword, "Count": count}
                   for hour, counts in hourly_data.items()
                   for keyword, count in counts.items()]
    hourly_counts_df = pd.DataFrame(hourly_rows)

    return original_df, hourly_counts_df

def create_dataframes(topic: list[str]) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Main function to extract data and return two DataFrames: original and hourly counts"""
    s3 = s3_connection()
    filenames = extract_bluesky_files(s3)
    return multi_threading_matching(s3, topic, filenames)

def main(topic: list[str]) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Extracts data from S3 Bucket and creates two summary DataFrames"""
    original_df, hourly_counts_df = create_dataframes(topic)
    return original_df, hourly_counts_df

if __name__ == "__main__":
    topics = ['good', 'bad']
    extracted_dataframe, hourly_counts_dataframe = main(topics)
    logging.info("\nExtracted Dataframe:\n%s", extracted_dataframe)
    logging.info("\nHourly Counts Dataframe:\n%s", hourly_counts_dataframe)
