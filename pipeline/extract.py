"""Extract script to connect to Bluesky Firehose and extract relevant data given topics"""

import csv
import ssl
import json
import re
import certifi

from atproto import CAR, models
from atproto_client.models.utils import get_or_create
from atproto_firehose import FirehoseSubscribeReposClient, parse_subscribe_repos_message


class JSONExtra(json.JSONEncoder):
    """Serializes raw objects (including CID-Content Identifier) as strings."""

    def default(self, obj: bytes):
        try:
            return json.JSONEncoder.default(self, obj)
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
    except (TypeError, AttributeError):
        return None


def get_firehose_data(message: bytes, topics: list[str],
                      csv_writer: csv.writer, csvfile: csv.writer) -> None:
    """Handles incoming messages, parses data, and writes to a CSV file based on dynamic topics."""
    commit = parse_subscribe_repos_message(message)
    if not isinstance(commit, models.ComAtprotoSyncSubscribeRepos.Commit):
        return
    car = CAR.from_bytes(commit.blocks)
    for op in commit.ops:
        if op.action in ["create"] and op.cid:
            raw = car.blocks.get(op.cid)
            cooked = get_or_create(raw, strict=False)

            if cooked.py_type == "app.bsky.feed.post":
                firehose_text = extract_text_from_bytes(raw)

                for topic in topics:
                    if topic in firehose_text:

                        csv_writer.writerow([firehose_text, topic])
                        csvfile.flush()
                        print(f"Written: {firehose_text}")
                        print("="*100)
                        break


def connect_and_write(topics: list[str]) -> None:
    """Connect to BlueSky Firehose API and write data to CSV."""
    ssl_context = ssl.create_default_context(cafile=certifi.where())

    client = FirehoseSubscribeReposClient()
    client.ssl_context = ssl_context

    with open('output.csv', mode='w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile, quotechar='"', quoting=csv.QUOTE_MINIMAL)
        header = ['text', 'keyword']

        writer.writerow(header)

        def start_firehose_extraction() -> None:
            """Sarts the Bluesky firehose extraction"""
            client.start(lambda message: get_firehose_data(
                message, topics, writer, csvfile))

        start_firehose_extraction()


def main() -> None:
    """Main function of the script to run Bluesky Firehouse extraction"""
    topics = ['cloud', 'sky']
    connect_and_write(topics)


if __name__ == "__main__":
    main()
