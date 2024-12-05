"""Uploads data from Bluesky firehose to S3"""

import logging
import ssl
import json
import re
import datetime
import os
import certifi
import boto3
from boto3 import client
from dotenv import load_dotenv
from psycopg2.extensions import connection
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
from atproto import CAR, models
from atproto_client.models.utils import get_or_create
from atproto_firehose import FirehoseSubscribeReposClient, parse_subscribe_repos_message


S3_CLIENT = boto3.client('s3')
load_dotenv(".env")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
    ]
)


class JSONExtra(json.JSONEncoder):
    """Serializes raw objects (including CID-Content Identifier) as strings."""

    def default(self, obj: bytes):
        try:
            return super().default(obj)
        except (TypeError, ValueError, KeyError):
            return repr(obj)


def s3_connection() -> connection:
    """Connects to an S3"""
    s3 = client("s3", aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
                aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"))
    return s3


def format_text(text: str) -> str:
    """Removes extra lines and whitespaces from text"""
    # Remove consecutive empty lines
    text = re.sub(r'\n\s*\n', '\n', text)

    text = text.strip()
    # Replace multiple spaces with a single space
    text = re.sub(r'\s+', ' ', text)
    return text


def extract_text_from_bytes(raw: bytes) -> str:
    """Extracts text from a raw Bluesky post"""
    try:
        json_data = json.dumps(raw, cls=JSONExtra, indent=2)
        parsed_json = json.loads(json_data)
        text = parsed_json.get('text')
        return format_text(text)
    except (TypeError, AttributeError) as e:
        logging.error("Error extracting text: %s", e)
        return None


def get_firehose_data(message: bytes) -> None:
    """Handles incoming messages, parses data, and writes to a CSV file based on topics."""
    repo_commit = parse_subscribe_repos_message(message)
    if not isinstance(repo_commit, models.ComAtprotoSyncSubscribeRepos.Commit):
        return
    car_file = CAR.from_bytes(repo_commit.blocks)
    for operation in repo_commit.ops:
        if operation.action == "create" and operation.cid:
            raw_bytes = car_file.blocks.get(operation.cid)
            processed_post = get_or_create(raw_bytes, strict=False)

            if not processed_post.py_type is None and processed_post.py_type == "app.bsky.feed.post":
                firehose_text = extract_text_from_bytes(raw_bytes)
                if firehose_text is not None:
                    logging.info('Extracted text: %s', firehose_text)
                    upload_to_s3(firehose_text)


def start_firehose_extraction(firehose_client: FirehoseSubscribeReposClient) -> None:
    """Starts the Bluesky firehose extraction"""
    firehose_client.start(lambda message: get_firehose_data(message))


def connect_and_upload() -> None:
    """Connect to BlueSky Firehose API and starts extracting from firehose."""
    logging.info("Starting Bluesky Firehose extraction")

    ssl_context = ssl.create_default_context(cafile=certifi.where())

    firehose_client = FirehoseSubscribeReposClient()
    firehose_client.ssl_context = ssl_context

    start_firehose_extraction(firehose_client)


def upload_to_s3(content: str) -> None:
    """Uploads text to S3 Bucket"""
    try:
        current_datetime = datetime.datetime.now()
        current_date = current_datetime.strftime("%Y-%m-%d")
        current_hour = current_datetime.strftime("%H")

        s3_bucket = os.environ.get("S3_BUCKET_NAME")
        s3_prefix = os.environ.get("S3_OBJECT_PREFIX")

        folder_path = f"{s3_prefix}{current_date}/{current_hour}/"
        timestamp = current_datetime.strftime("%Y%m%d%H%M%S%f")
        s3_key = f"{folder_path}{timestamp}.txt"

        s3_client = s3_connection()

        s3_client.put_object(Bucket=s3_bucket, Key=s3_key, Body=content)
        logging.info("Uploaded to S3: %s", s3_key)
    except (NoCredentialsError, PartialCredentialsError) as e:
        logging.error("S3 credentials error: %s", e)


if __name__ == "__main__":
    connect_and_upload()
