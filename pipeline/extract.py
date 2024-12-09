"""Extracts data from S3 Bucket"""

import os
import logging
import json
import datetime

from pytz import HOUR
from httpx import Client
import pandas as pd
from boto3 import client
from dotenv import load_dotenv
from pytrends.request import TrendReq
from botocore.config import Config

load_dotenv(".env")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)


def s3_connection() -> client:
    """Connects to an S3 and configs S3 Connection"""
    config = Config(
        connect_timeout=5,
        read_timeout=10,
        retries={
            'max_attempts': 3,
            'mode': 'standard'
        },
        max_pool_connections=110
    )
    try:
        aws_access_key = os.environ.get("AWS_ACCESS_KEY_ID")
        aws_secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")

        if not aws_access_key or not aws_secret_key:
            logging.error("Missing required AWS credentials in .env file.")
        s3 = client(
            "s3",
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            config=config
        )
    except ConnectionError as e:
        logging.error('An error occurred attempting to connect to S3: %s', e)
        return None
    return s3


def average_sentiment_analysis(keyword: str, file_data: dict) -> tuple:
    """Calculates the average sentiment for a keyword in a .json file"""
    total_sentiment = 0
    mentions = 0
    for text, sentiment in file_data.items():
        if keyword in text:
            total_sentiment += sentiment['Sentiment Score']['compound']
            mentions += 1
    if mentions == 0:
        return (total_sentiment, mentions)
    return total_sentiment/mentions, mentions


def extract_s3_data(s3: Client, bucket: str, topic: list[str]) -> pd.DataFrame:
    """Extracts relevant data from an S3 Bucket for the past 7 days."""
    today = datetime.datetime.now()
    date_list = [(today - datetime.timedelta(days=i)).strftime("%Y-%m-%d")
                 for i in range(7)]

    sentiment_and_mention_data = []

    for date in date_list:
        prefix = f"bluesky/{date}/"
        response = s3.list_objects_v2(
            Bucket=bucket, Prefix=prefix, Delimiter='/')
        logging.info(prefix.split('/')[1])
        if 'Contents' in response:
            for obj in response['Contents']:
                key = obj['Key']
                hour = key.split("/")[-1].split(".")[0]

                if key.endswith('.json') and key.count('/') == prefix.count('/'):
                    file_obj = s3.get_object(Bucket=bucket, Key=key)
                    file_content = json.loads(
                        file_obj['Body'].read().decode('utf-8'))

                    for keyword in topic:
                        sentiment_and_mentions = average_sentiment_analysis(
                            keyword, file_content)

                        sentiment_and_mention_data.append({
                            'Date and Hour': f"{date} {hour}",
                            'Keyword': keyword,
                            'Average Sentiment': sentiment_and_mentions[0],
                            'Total Mentions': sentiment_and_mentions[1]
                        })
        else:
            logging.info(f"No files found in the folder for date {date}.")

    if sentiment_and_mention_data:
        return pd.DataFrame(sentiment_and_mention_data)

    logging.info("No files found in the past 7 days.")
    raise ValueError("No files found in the past 7 days.")


def initialize_trend_request() -> TrendReq:
    """Initialize and return a TrendReq object."""
    return TrendReq()


def fetch_suggestions(pytrend: TrendReq, keyword: str) -> list[dict]:
    """Fetch and print suggestions for a given keyword."""
    return pytrend.suggestions(keyword=keyword)


def main(topic: list[str]) -> pd.DataFrame:
    """Main function to run extract script"""
    s3 = s3_connection()

    bucket = os.environ.get("S3_BUCKET_NAME")

    extracted_dataframe = extract_s3_data(s3, bucket, topic)

    pytrend = initialize_trend_request()
    for keyword in topic:
        extracted_dataframe.loc[extracted_dataframe['Keyword'] == keyword,
                                'Related Terms'] = ",".join([suggestion['title']
                                                             for suggestion in fetch_suggestions(pytrend, keyword)])
    return extracted_dataframe

if __name__ == "__main__":
    main('hi')