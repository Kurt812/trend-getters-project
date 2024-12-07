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
    Fetch raw JSON data from S3 and convert it into a DataFrame.
    """
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=bucket_name, Key=file_key)
    raw_data = json.loads(obj['Body'].read().decode(
        'utf-8'))  # Parse JSON content

    # Convert JSON into a DataFrame format
    data = []
    for record in raw_data:  # Assumes the file contains a list of records
        data.append({
            'text': record['Text'],
            # Extract compound score
            'sentiment_score': record['Sentiment Score']['compound'],
            'keywords': record['Keywords']
        })

    return pd.DataFrame(data)


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
            host=ENV["DB_HOST"],
            port=ENV["DB_PORT"],
            database=ENV["DB_NAME"]
        )
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.execute(f"SET SEARCH_PATH TO {ENV['SCHEMA_NAME']};")
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


def store_metrics_in_rds(metrics: pd.DataFrame, conn):
    """
    Store keyword metrics in the RDS database.
    """
    cursor = conn.cursor()
    now = datetime.now()

    for _, row in metrics.iterrows():
        cursor.execute("""
            INSERT INTO keyword_metrics (keyword, mentions_count, avg_sentiment, last_updated)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (keyword) DO UPDATE
            SET mentions_count = EXCLUDED.mentions_count,
                avg_sentiment = EXCLUDED.avg_sentiment,
                last_updated = EXCLUDED.last_updated;
        """, (row['keyword'], row['mentions_count'], row['avg_sentiment'], now))

    conn.commit()


def run_etl():
    # Fetch raw data from S3
    raw_data = fetch_raw_data(file_key='raw_data.json',
                              bucket_name='your-bucket')

    # Load predefined keywords
    with open('config/keywords.txt', 'r') as f:
        keywords = [line.strip() for line in f]

    # Calculate metrics
    metrics = calculate_metrics(raw_data, keywords)

    # Store metrics in RDS
    conn = setup_connection()
    store_metrics_in_rds(metrics, conn)
    conn.close()
