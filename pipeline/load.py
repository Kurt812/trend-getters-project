"""Script to load data into RDS"""
from os import environ as ENV
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from extract import main as downloader
from transform import main as optimus_prime


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


def insert_keywords(conn, cursor, topics: list[str]) -> list[int]:
    """Insert keywords into keywords table from topic"""
    keyword_ids = []
    for keyword in topics:
        cursor.execute(
            "SELECT keywords_id FROM keywords WHERE keyword = %s", (keyword,))
        existing_id = cursor.fetchone()

        if existing_id is None:
            cursor.execute("""INSERT INTO keywords (keyword)
                            VALUES (%s) RETURNING keywords_id""", (keyword,))
            conn.commit()
            new_keyword_id = cursor.fetchone()['keywords_id']
            keyword_ids.append(new_keyword_id)
            continue

        keyword_ids.append(existing_id['keywords_id'])

    return keyword_ids


def main(topics: list[str]):
    """Main function to load environment variables to import data into the database."""
    conn, cursor = setup_connection()
    load_dotenv()
    keyword_ids = insert_keywords(conn, cursor, topics)
    print(keyword_ids)


if __name__ == "__main__":
    topics = ['strawberries', 'chocolate']  # this
    main(topics)
    extracted_dataframe = downloader(topics)
    final_dataframe = optimus_prime(extracted_dataframe)
    final_dataframe.to_csv("output.csv", index=False)
