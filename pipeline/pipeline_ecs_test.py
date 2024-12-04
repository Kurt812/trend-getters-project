import csv
import ssl
import json
import re
import logging
import os
from datetime import datetime
import certifi
import psycopg2
import psycopg2.extras
from dotenv import dotenv_values

from atproto import CAR, models
from atproto_firehose import FirehoseSubscribeReposClient, parse_subscribe_repos_message

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
    ]
)


def format_text(text: str) -> str:
    """Cleans up text."""
    text = re.sub(r'\n\s*\n', '\n', text)  # Remove consecutive empty lines
    text = text.strip()
    # Replace multiple spaces with a single space
    return re.sub(r'\s+', ' ', text)


def extract_text_from_bytes(raw: bytes) -> str:
    """Extracts text from a raw Bluesky post."""
    try:
        parsed_json = json.loads(json.dumps(raw, cls=json.JSONEncoder))
        return format_text(parsed_json.get('text', ''))
    except Exception as e:
        logging.error(f"Error extracting text: {e}")
        return None


def get_firehose_data(message: bytes, topics: list[str], cursor, connection) -> None:
    """Parses incoming messages and inserts keywords into the database."""
    repo_commit = parse_subscribe_repos_message(message)
    if not isinstance(repo_commit, models.ComAtprotoSyncSubscribeRepos.Commit):
        return
    car_file = CAR.from_bytes(repo_commit.blocks)
    for operation in repo_commit.ops:
        if operation.action == "create" and operation.cid:
            raw_bytes = car_file.blocks.get(operation.cid)
            processed_post = raw_bytes.get("text") if raw_bytes else None
            if not processed_post:
                continue

            firehose_text = extract_text_from_bytes(raw_bytes)
            if not firehose_text:
                continue

            for keyword in topics:
                if keyword in firehose_text:
                    logging.info(
                        f"Found keyword '{keyword}' in post: {firehose_text}")
                    insert_keyword_into_db(
                        firehose_text, keyword, cursor, connection)
                    break


def connect_to_database() -> tuple:
    """Sets up a connection to the database."""
    connection = psycopg2.connect(
        user=os.environ["DATABASE_USERNAME"],
        password=os.environ["DATABASE_PASSWORD"],
        host=os.environ["DATABASE_IP"],
        port=os.environ["DATABASE_PORT"],
        database=os.environ["DATABASE_NAME"]
    )
    return connection, connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)


def insert_keyword_into_db(post_text: str, keyword: str, cursor, connection) -> None:
    """Inserts a post and keyword into the database."""
    cursor.execute(
        "SET search_path TO trendgineers;")
    cursor.execute(
        """INSERT INTO keywords (keyword) VALUES (%s)
        ON CONFLICT DO NOTHING;""",
        (keyword,)
    )
    connection.commit()


def connect_and_listen(topics: list[str]) -> None:
    """Connects to Bluesky Firehose and processes incoming data."""
    logging.info("Starting Firehose connection...")
    ssl_context = ssl.create_default_context(cafile=certifi.where())
    client = FirehoseSubscribeReposClient()
    client.ssl_context = ssl_context

    connection, cursor = connect_to_database()

    try:
        client.start(lambda message: get_firehose_data(
            message, topics, cursor, connection))
    except KeyboardInterrupt:
        logging.info("Stopping Firehose connection...")
    finally:
        cursor.close()
        connection.close()


def main() -> None:
    """Main function."""
    topics = ['christmas', 'hi', 'firehose']  # Add your desired keywords
    connect_and_listen(topics)


if __name__ == "__main__":
    main()
