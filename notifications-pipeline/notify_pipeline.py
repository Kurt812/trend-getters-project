"""Script to update keyword recordings for subscribed to keywords"""

# pylint: disable=E0401

from os import environ as ENV
import logging
import requests
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor, RealDictRow
from psycopg2.extensions import cursor as Cursor

load_dotenv()

API_ENDPOINT = ENV["API_ENDPOINT"]


def get_connection() -> tuple:
    """Establish and return a database connection"""
    conn = psycopg2.connect(
        user=ENV["DB_USERNAME"],
        password=ENV["DB_PASSWORD"],
        host=ENV["DB_HOST"],
        port=ENV["DB_PORT"],
        database=ENV["DB_NAME"]
    )
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute("SET SEARCH_PATH TO %s;",
                   (ENV["SCHEMA_NAME"],))
    return conn, cursor


def submit_topic(data: dict) -> None:
    """Submit topic details to the API"""
    try:
        response = requests.post(API_ENDPOINT, json=data, timeout=1000)
        if response.status_code == 200:
            logging.info("✅ Topic submitted successfully!")
        else:
            logging.error("Error: %s", response.json().get(
                'message', 'Unknown error'))
    except requests.exceptions.RequestException as e:
        logging.error("Failed to connect to the API. Error: %s", e)


def find_unique_keywords(cursor: Cursor) -> RealDictRow:
    """Finds the keywords that users have subscribed to"""
    cursor.execute(
        """SELECT DISTINCT keywords_id
         FROM subscription""")
    keywords = cursor.fetchall()
    return keywords


def fetch_keyword(cursor: Cursor, keyword_id: int) -> RealDictRow:
    """Find keyword name from ID"""
    cursor.execute(
        """SELECT keyword
         FROM keywords
         WHERE keywords_id = %s""", (keyword_id,))
    keywords = cursor.fetchone()
    return keywords


def main() -> None:
    """Run ETL through API for each keyword"""
    _, cursor = get_connection()
    keywords = find_unique_keywords(cursor)
    for keyword_id in keywords:
        keyword = fetch_keyword(cursor, keyword_id["keywords_id"])["keyword"]
        topic_data = {"topic_name": keyword}
        submit_topic(topic_data)


if __name__ == "__main__":
    main()
