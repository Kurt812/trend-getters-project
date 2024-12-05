"""Script to load data into RDS"""

from os import environ as ENV
import pandas as pd
import psycopg2
import psycopg2.extras
from psycopg2.extensions import connection, cursor
from dotenv import load_dotenv
from extract import main as extract_main
from transform import main as transform_main


def setup_connection():
    """Retrieve database connection and cursor"""
    conn = psycopg2.connect(
        user=ENV["DB_USERNAME"],
        password=ENV["DB_PASSWORD"],
        host=ENV["DB_HOST"],
        port=ENV["DB_PORT"],
        database=ENV["DB_NAME"]
    )
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cursor.execute(f"SET SEARCH_PATH TO {ENV['SCHEMA_NAME']};")
    return conn, cursor


def insert_keywords(conn: connection, cursor: cursor, 
                    topics: list[str]) -> None:
    """Insert keywords into keywords table from topic"""
    for keyword in topics:
        cursor.execute(
            "SELECT keywords_id FROM keywords WHERE keyword = %s", (keyword,))
        existing_id = cursor.fetchone()

        if existing_id is None:
            cursor.execute("""INSERT INTO keywords (keyword)
                            VALUES (%s) RETURNING keywords_id""", (keyword,))
            conn.commit()


def insert_keyword_recordings(conn: connection, 
                              cursor: cursor, dataframe: pd.DataFrame) -> None:
    """Inserts data into the keyword_recordings table"""

    for row in dataframe.to_dict(orient='records'):
        hour = row['Hour']
        total_mentions = row['Count']
        average_sentiment = row['Average Sentiment']
        keyword_id = row['keyword_id']
        cursor.execute("""INSERT INTO keyword_recordings 
                       (keywords_id, total_mentions, avg_sentiment, hour_of_day)
                       VALUES (%s, %s, %s, %s)""", (keyword_id, total_mentions, average_sentiment, hour))
        conn.commit()


def insert_related_terms(conn: connection, cursor: cursor):
    """Inserts unique related terms into the related terms table"""
    dataframe = pd.read_csv("output.csv")
    keyword_and_ids = {}
    processed_terms = set()  # Track unique terms

    for row in dataframe.to_dict(orient='records'):
        related_terms = row['Related Terms'].split(",")
        keyword = row['Keyword']
        
        for term in related_terms:
            term = term.strip()
            if term not in processed_terms:
                cursor.execute("SELECT related_term_id FROM related_terms WHERE related_term = %s", (term, ))
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


def get_keyword_id(cursor: cursor, keyword: str) -> int:
    """Returns the keyword id for a given keyword"""
    cursor.execute("""SELECT keywords.keywords_id FROM keywords WHERE keyword = %s""", (keyword, ))
    return cursor.fetchone()['keywords_id']


def insert_related_term_assignment(conn: connection, cursor: cursor, keyword_and_ids: dict) -> None:
    """Inserts data into the related_term_assignment table"""
    for key, value in keyword_and_ids.items():
        keyword_id = get_keyword_id(cursor, value)
        cursor.execute("""INSERT INTO related_term_assignment (keywords_id, related_term_id)
                       VALUES (%s, %s)""", (keyword_id, key))
        conn.commit()

def main(topics: list[str], extracted_dataframe: pd.DataFrame):
    """Main function to load environment variables to import data into the database."""
    conn, cursor = setup_connection()
    load_dotenv()
    insert_keywords(conn, cursor, topics)
    insert_keyword_recordings(conn,cursor, extracted_dataframe)
    related_term_ids = insert_related_terms(conn,cursor)
    insert_related_term_assignment(conn,cursor,related_term_ids)


if __name__ == "__main__":
    topics = ['strawberries', 'clouds']  # this
    main(topics)

    # extracted_dataframe = extract_main(topics)
    # final_dataframe = transform_main(extracted_dataframe)

    # final_dataframe.to_csv("output.csv", index=False)
