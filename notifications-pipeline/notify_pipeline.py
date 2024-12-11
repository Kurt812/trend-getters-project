"""Script to update keyword recordings for subscribed to keywords"""

# pylint: disable=E0401

from os import environ as ENV
import requests
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor

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
            print("✅ Topic submitted successfully!")
        else:
            print(f"""Error: {response.json().get(
                'message', 'Unknown error')}""")
    except requests.exceptions.RequestException as e:
        print(f"Failed to connect to the API. Error: {e}")

def find_unique_keywords(cursor):
    """Fetch keywords that have been subscribed to"""
    cursor.execute(
        """SELECT DISTINCT keywords_id
         FROM subscription""")
    keywords = cursor.fetchall()
    return keywords

def main():
    """Run ETL through API for each keyword"""
    _, cursor = get_connection()
    keywords = find_unique_keywords(cursor)
    for keyword in keywords:
        topic_data = {"topic_name": keyword}
        submit_topic(topic_data)

if __name__ == "__main__":
    main()
