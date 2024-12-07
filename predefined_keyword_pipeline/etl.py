import psycopg2
import logging
from datetime import datetime
import json
import boto3
import pandas as pd
from os import environ as ENV
from dotenv import load_dotenv
import psycopg2.extras
from psycopg2 import OperationalError, InterfaceError, DatabaseError

load_dotenv()


def fetch_raw_data(file_key: str, bucket_name: str) -> pd.DataFrame:
    """
    Fetch raw JSON data containing a single record from S3 and convert it into a DataFrame.
    """
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=bucket_name, Key=file_key)
    raw_data = json.loads(obj['Body'].read().decode(
        'utf-8'))  # Parse JSON content

    # Extract fields from the single record
    data = {
        'text': raw_data['Text'],
        # Extract compound score
        'sentiment_score': raw_data['Sentiment Score']['compound'],
        'keywords': raw_data['Keywords']
    }

    # Convert the single record to a DataFrame
    return pd.DataFrame([data])


def calculate_metrics(df: pd.DataFrame, keywords: list) -> pd.DataFrame:
    """
    Calculate metrics for each keyword.
    """
    results = []

    for keyword in keywords:
        # Filter rows where the keyword exists in the 'keywords' dictionary
        keyword_rows = df[df['keywords'].apply(lambda kw: keyword in kw)]

        if not keyword_rows.empty:
            # Calculate metrics
            mention_count = keyword_rows['keywords'].apply(
                lambda kw: kw[keyword]).sum()
            avg_sentiment = keyword_rows['sentiment_score'].mean()

            # Append results
            results.append({
                'keyword': keyword,
                'mentions_count': mention_count,
                'avg_sentiment': avg_sentiment
            })

    return pd.DataFrame(results)


def setup_connection() -> None:
    """Sets up a connection to the RDS"""
    try:
        conn = psycopg2.connect(
            user=ENV["DB_USERNAME"],
            password=ENV["DB_PASSWORD"],
            host=ENV["DB_HOST_2"],
            port=ENV["DB_PORT"],
            database=ENV["DB_NAME_2"]
        )
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        # cursor.execute(f"SET SEARCH_PATH TO {ENV['SCHEMA_NAME']};")
        return conn
    except OperationalError as oe:
        logging.error(
            "Operational error while connecting to the database: %s", oe)
        raise
    except InterfaceError as ie:
        logging.error(
            "Interface error while connecting to the database: %s", ie)
        raise
    except DatabaseError as de:
        logging.error("Database error occurred: %s", de)
        raise
    except Exception as e:
        logging.error(
            "Unexpected error while setting up the database connection: %s", e)
        raise


def get_keywords_id(keyword, cursor):
    cursor.execute(
        f"SELECT keywords_id FROM keywords WHERE keyword='{keyword}'")
    result = cursor.fetchone()
    return result


def store_metrics_in_rds(metrics: pd.DataFrame, conn):
    """
    Store keyword metrics in the RDS database.
    """
    cursor = conn.cursor()
    now = datetime.now()

    for _, row in metrics.iterrows():
        keyword_id = get_keywords_id(row['keyword'], cursor)
        cursor.execute("""
            INSERT INTO keyword_recordings (keywords_id, total_mentions, avg_sentiment, hour_of_day)
            VALUES (%s, %s, %s, %s)
        """, (keyword_id, row['mentions_count'], row['avg_sentiment'], now))

    conn.commit()


def fetch_keywords_from_db(conn) -> set:
    """
    Fetch existing keywords from the database and return them as a set.
    """
    cursor = conn.cursor()
    cursor.execute("SELECT keyword FROM keywords")
    # Convert to a set for fast lookup
    keywords = {row[0] for row in cursor.fetchall()}
    return keywords


def add_keywords_to_db(new_keywords: set, conn):
    """
    Add new keywords to the database.
    """
    cursor = conn.cursor()
    for keyword in new_keywords:
        try:
            cursor.execute(
                "INSERT INTO keywords (keyword) VALUES (%s) ON CONFLICT DO NOTHING",
                (keyword,)
            )
        except Exception as e:
            print(f"Failed to add keyword {keyword}: {e}")
    conn.commit()


def process_keywords_in_text(df: pd.DataFrame, db_keywords: set, conn):
    """
    Identify keywords in text that are not in the database, add them, and return all keywords.
    """
    all_keywords_in_text = set()

    for _, row in df.iterrows():
        # Extract keywords from the 'keywords' column (assuming it's a dictionary)
        text_keywords = set(row['keywords'].keys())
        all_keywords_in_text.update(text_keywords)

    # Find missing keywords
    new_keywords = all_keywords_in_text - db_keywords

    # Add missing keywords to the database
    if new_keywords:
        add_keywords_to_db(new_keywords, conn)

    # Return the updated set of keywords
    return db_keywords | new_keywords


def run_etl():
    """
    Run the ETL pipeline.
    """
    # Fetch raw data from S3
    raw_data = fetch_raw_data(
        file_key="20241206230050607806.json", bucket_name="trendgineers-test-bucket")

    # Connect to the database
    conn = setup_connection()

    # Fetch existing keywords from the database
    db_keywords = fetch_keywords_from_db(conn)

    # Process text to identify and add missing keywords
    all_keywords = process_keywords_in_text(raw_data, db_keywords, conn)

    # Calculate metrics for all keywords
    metrics = calculate_metrics(raw_data, all_keywords)

    # Store metrics back into the database
    store_metrics_in_rds(metrics, conn)

    conn.close()


if __name__ == "__main__":
    run_etl()
