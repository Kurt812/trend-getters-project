"""Extract script to connect to Bluesky Firehose and extract relevant data given topics"""

# pylint: disable=W1203

import ssl
import json
import re
import logging
import os
import boto3
import certifi
import datetime
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

from atproto import CAR, models
from atproto_client.models.utils import get_or_create
from atproto_firehose import FirehoseSubscribeReposClient, parse_subscribe_repos_message

S3_BUCKET_NAME = "trendgineers-raw-firehose-data"
S3_OBJECT_PREFIX = "bluesky/"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
    ]
)

s3_client = boto3.client('s3')


def upload_to_s3(content: str, key: str) -> None:
    """Uploads text to S3 Bucket"""
    try:
        s3_client.put_object(Bucket=S3_BUCKET_NAME, Key=key, Body=content)
        logging.info(f"Uploaded to S3: {key}")
    except (NoCredentialsError, PartialCredentialsError) as e:
        logging.error(f"S3 credentials error: {e}")
    except Exception as e:
        logging.error(f"Error uploading to S3: {e}")


class JSONExtra(json.JSONEncoder):
    """Serializes raw objects (including CID-Content Identifier) as strings."""

    def default(self, obj: bytes):
        try:
            return super().default(obj)
        except (TypeError, ValueError, KeyError):
            return repr(obj)


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


def get_firehose_data(message: bytes, topics: list[str]) -> None:
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

                # Upload to S3
                upload_to_s3(firehose_text, s3_key)


def connect_and_upload(topics: list[str]) -> None:
    """Connect to BlueSky Firehose API and starts extracting from Firehose."""
    logging.info(f"Starting Bluesky Firehose extraction for topics: {topics}")

    ssl_context = ssl.create_default_context(cafile=certifi.where())
    client = FirehoseSubscribeReposClient()
    client.ssl_context = ssl_context

    def start_firehose_extraction() -> None:
        """Starts the Bluesky firehose extraction"""
        client.start(lambda message: get_firehose_data(message, topics))

    start_firehose_extraction()


def main() -> None:
    """Main function of the script to run Bluesky Firehouse extraction"""
    topics = ['cloud', 'sky']
    connect_and_upload(topics)


if __name__ == "__main__":
    main()
