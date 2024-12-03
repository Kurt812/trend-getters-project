"""Extract script to connect to Bluesky Firehose and extract relevant data given topics"""

import csv
import ssl
import json
import re
import certifi
import logging
import os
from datetime import datetime

from atproto import CAR, models
from atproto_client.models.utils import get_or_create
from atproto_firehose import FirehoseSubscribeReposClient, parse_subscribe_repos_message

HEADER = ['text', 'keyword']
OUTPUT_FOLDER = "bluesky_output_data"

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
        except (TypeError, ValueError, KeyError) as e:
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
        print(raw)
        json_data = json.dumps(raw, cls=JSONExtra, indent=2)
        parsed_json = json.loads(json_data)
        text = parsed_json.get('text')
        return format_text(text)
    except (TypeError, AttributeError) as e:
        logging.error(f"Error extracting text: {e}")
        return None


def get_firehose_data(message: bytes, topics: list[str],
                      csv_writer: csv.writer, csvfile: csv.writer) -> None:
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

                for keyword in topics:
                    if keyword in firehose_text:

                        csv_writer.writerow([firehose_text, keyword])
                        csvfile.flush()
                        logging.info(
                            f"Written post containing keyword: '{keyword}', post: {firehose_text}")
                        print("="*100)
                        break


def connect_and_write(topics: list[str]) -> None:
    """Connect to BlueSky Firehose API and write data to CSV."""
    logging.info(f"Starting Bluesky Firehose extraction for topics: {topics}")

    os.makedirs(OUTPUT_FOLDER, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_filename = os.path.join(
        OUTPUT_FOLDER, f"bluesky_output_{timestamp}.csv")

    logging.info(f"Saving CSV output to: {csv_filename}")
    ssl_context = ssl.create_default_context(cafile=certifi.where())
    client = FirehoseSubscribeReposClient()
    client.ssl_context = ssl_context

    with open(csv_filename, mode='w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile, quotechar='"', quoting=csv.QUOTE_MINIMAL)
        logging.info(f"Created CSV output file: {csv_filename}")

        writer.writerow(HEADER)

        def start_firehose_extraction() -> None:
            """Starts the Bluesky firehose extraction"""
            client.start(lambda message: get_firehose_data(
                message, topics, writer, csvfile))

        start_firehose_extraction()


def main() -> None:
    """Main function of the script to run Bluesky Firehouse extraction"""
    topics = ['cloud', 'sky']
    connect_and_write(topics)


if __name__ == "__main__":
    main()
