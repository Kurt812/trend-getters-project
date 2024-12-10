"""Script to load data into RDS"""

import datetime
from os import environ as ENV
import logging
import pandas as pd
from datetime import datetime
import psycopg2
import psycopg2.extras
from psycopg2.extensions import connection as connect, cursor as curs
from dotenv import load_dotenv


def setup_connection() -> tuple:
    """Retrieve database connection and cursor"""
    try:
        conn = psycopg2.connect(
            user=ENV["DB_USERNAME"],
            password=ENV["DB_PASSWORD"],
            host=ENV["DB_HOST"],
            port=ENV["DB_PORT"],
            database=ENV["DB_NAME"]
        )
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.execute(f"SET SEARCH_PATH TO {ENV['SCHEMA_NAME']};")
    except psycopg2.OperationalError as e:
        logging.error(
            "Operational error while connecting to the database: %s", e)
        raise
    except Exception as e:
        logging.error("Error connecting to database: %s", e)
        raise
    logging.info("Connection successfully established to database.")

    return conn, cursor


def insert_keywords(conn: connect, cursor: curs,
                    topic: list[str]) -> None:
    """Insert keywords into keywords table from topic"""
    for keyword in topic:
        cursor.execute(
            "SELECT keywords_id FROM keywords WHERE keyword = %s", (keyword,))
        existing_id = cursor.fetchone()

        if existing_id is None:
            cursor.execute(
                """INSERT INTO keywords (keyword) VALUES (%s)""", (keyword,))
            conn.commit()


def insert_keyword_recordings(conn: connect,
                              cursor: curs, dataframe: pd.DataFrame) -> None:
    """Inserts data into the keyword_recordings table"""

    for row in dataframe.to_dict(orient='records'):
        date_and_hour = datetime.strptime(row['Date and Hour'], "%Y-%m-%d %H")
        total_mentions = row['Total Mentions']
        average_sentiment = row['Average Sentiment']
        keyword_id = row['keyword_id']
        cursor.execute("""INSERT INTO keyword_recordings
                       (keywords_id, total_mentions, avg_sentiment, date_and_hour)
                       VALUES (%s, %s, %s, %s)""",
                       (keyword_id, total_mentions, average_sentiment, date_and_hour))
        conn.commit()


def insert_related_terms(conn: connect, cursor: curs, extracted_dataframe: pd.DataFrame) -> dict:
    """Inserts unique related terms into the related terms table"""
    keyword_and_ids = {}
    processed_terms = set()

    for row in extracted_dataframe.to_dict(orient='records'):
        related_terms = row['Related Terms'].split(",")
        keyword = row['Keyword']

        for term in related_terms:
            term = term.strip()
            if term not in processed_terms:
                cursor.execute("""SELECT related_term_id FROM related_terms
                               WHERE related_term = %s""", (term, ))
                existing_term = cursor.fetchone()

                if existing_term is None:
                    cursor.execute("""INSERT INTO related_terms (related_term)
                                VALUES (%s) RETURNING related_term_id""", (term, ))
                    related_term_id = cursor.fetchone()['related_term_id']
                else:
                    related_term_id = existing_term['related_term_id']

                keyword_and_ids[related_term_id] = keyword
                processed_terms.add(term)
                conn.commit()

    return keyword_and_ids


def get_keyword_id(cursor: curs, keyword: str) -> int:
    """Returns the keyword id for a given keyword"""

    cursor.execute(
        """SELECT keywords.keywords_id FROM keywords WHERE keyword = %s""", (keyword, ))
    result = cursor.fetchone()
    if result is None:
        logging.error("Keyword '%s' not found in the database.", keyword)
        raise ValueError(f"Keyword '{keyword}' not found.")
    return result['keywords_id']


def insert_related_term_assignment(conn: connect, cursor: curs, keyword_and_ids: dict) -> None:
    """Inserts data into the related_term_assignment table"""
    for key, value in keyword_and_ids.items():
        keyword_id = get_keyword_id(cursor, value)
        cursor.execute("""INSERT INTO related_term_assignment (keywords_id, related_term_id)
                       VALUES (%s, %s)""", (keyword_id, key))
        conn.commit()


def main(topic: list[str], extracted_dataframe: pd.DataFrame) -> None:
    """Main function to load environment variables to import data into the database."""
    conn, cursor = setup_connection()
    load_dotenv()
    insert_keywords(conn, cursor, topic)
    insert_keyword_recordings(conn, cursor, extracted_dataframe)
    related_term_ids = insert_related_terms(conn, cursor, extracted_dataframe)
    insert_related_term_assignment(conn, cursor, related_term_ids)


if __name__ == "__main__":
    topics = ['strawberries', 'clouds']
