import json
import re
import logging
import os
from datetime import datetime
import ssl
import certifi
import pandas as pd
import time

from atproto import CAR, models
from atproto_client.models.utils import get_or_create
from atproto_firehose import FirehoseSubscribeReposClient, parse_subscribe_repos_message

HEADER = ['text', 'keyword']

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


def get_firehose_data(message: bytes, topics: list[str], data: list[list[str]], start_time: float) -> None:
    """Handles incoming messages, parses data, and appends to the data list."""
    # Stop extraction if more than 60 seconds have passed
    if time.time() - start_time > 5:
        logging.info("Time limit reached. Stopping data collection.")
        raise StopIteration

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

                for keyword in topics:
                    if keyword in firehose_text:
                        data.append([firehose_text, keyword])
                        logging.info(
                            f"Added post containing keyword: '{keyword}', post: {firehose_text}")
                        break


def connect_and_collect(topics: list[str]) -> pd.DataFrame:
    """Connect to BlueSky Firehose API and collect data into a DataFrame."""
    logging.info(f"Starting Bluesky Firehose extraction for topics: {topics}")

    logging.info(f"Collecting data for topics: {topics}")
    ssl_context = ssl.create_default_context(cafile=certifi.where())

    client = FirehoseSubscribeReposClient()
    client.ssl_context = ssl_context

    data = []  # List to store rows for the DataFrame

    start_time = time.time()

    def message_handler(message):
        nonlocal start_time
        if time.time() - start_time > 5:
            client.stop()
            return
        get_firehose_data(message, topics, data, start_time)

    client.start(message_handler)

    return pd.DataFrame(data, columns=HEADER)


def main(topics: list[str]) -> pd.DataFrame:
    """Main function of the script to run Bluesky Firehose extraction."""
    df = connect_and_collect(topics)
    return df


if __name__ == "__main__":
    topics = ['cloud', 'sky']
    result_df = main(topics)
    print(result_df)  # Prints the first few rows of the DataFrame
