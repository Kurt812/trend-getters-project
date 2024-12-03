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

S3_BUCKET_NAME = "trendgineers-raw-firehose-data"
S3_OBJECT_PREFIX = "bluesky/"
S3_CLIENT = boto3.client('s3')
load_dotenv(".env")


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
        logging.error(f"Error extracting text: {e}")
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
                if not firehose_text:
                    continue
                logging.info(f'Extracted text: {firehose_text}')
                timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S%f")
                s3_key = f"{S3_OBJECT_PREFIX}{timestamp}.txt"

                upload_to_s3(firehose_text, s3_key)


def connect_and_upload() -> None:
    """Connect to BlueSky Firehose API and starts extracting from firehose."""
    logging.info(f"Starting Bluesky Firehose extraction")

    ssl_context = ssl.create_default_context(cafile=certifi.where())

    client = FirehoseSubscribeReposClient()
    client.ssl_context = ssl_context

    def start_firehose_extraction() -> None:
        """Starts the Bluesky firehose extraction"""
        client.start(lambda message: get_firehose_data(message))

    start_firehose_extraction()


def upload_to_s3(content: str, key: str) -> None:
    """Uploads text to S3 Bucket"""
    try:
        s3_client = s3_connection()
        s3_client.put_object(Bucket=S3_BUCKET_NAME, Key=key, Body=content)
        logging.info(f"Uploaded to S3: {key}")
    except (NoCredentialsError, PartialCredentialsError) as e:
        logging.error(f"S3 credentials error: {e}")
    except Exception as e:
        logging.error(f"Error uploading to S3: {e}")


def main():
    """Main function that uploads to S3"""
    connect_and_upload()


if __name__ == "__main__":
    main()
