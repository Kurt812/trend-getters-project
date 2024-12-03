"""This script cleans the data, matches keyword ids and sentiment scores."""

# pylint: disable=W0621
# pylint: disable=E0401

import logging
import pandas as pd
import psycopg2
import psycopg2.extras
from dotenv import dotenv_values
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer


# Load csv created by extract into relevant data structure e.g. pd.Dataframe
# Datacleaning
# Match mention to topic_id - will need to get topic_id from rds
# Sentiment analysis

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
    ]
)


def get_cursor(config):
    """Importing the data into the database"""
    conn = psycopg2.connect(
        user=config["DB_USERNAME"],
        password=config["DB_PASSWORD"],
        host=config["DB_HOST"],
        port=config["DB_PORT"],
        database=config["DB_NAME"]
    )
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    return cursor, conn


def clean_data(bluesky_data: pd.DataFrame, keywords: list[str]) -> pd.DataFrame:
    """Removes any rows that don't include the keywords and removes duplicate rows."""

    filtered_bluesky = bluesky_data[bluesky_data['text'].str.contains(
        r'\b(?:' + '|'.join(keywords) + r')\b', case=False, na=False, regex=True)]
    filtered_bluesky = bluesky_data[bluesky_data['keyword'].notnull()]
    filtered_bluesky = bluesky_data.drop_duplicates()

    return filtered_bluesky


def ensure_keywords_in_db(keywords: list, cursor, connection):
    """Ensure all keywords are present in the database. Add missing keywords."""
    cursor.execute("SET search_path TO trendgineers;")
    cursor.execute("SELECT keyword, keywords_id FROM keywords")
    rows = cursor.fetchall()

    # Convert rows into a dictionary
    keyword_map = {row['keyword']: row['keywords_id'] for row in rows}

    for keyword in keywords:
        keyword_lower = keyword.lower()  # Normalize for case-insensitivity
        if keyword_lower not in keyword_map:
            cursor.execute(
                "INSERT INTO keywords (keyword) VALUES (%s) RETURNING keywords_id",
                (keyword_lower,)
            )
            connection.commit()
            new_keyword_id = cursor.fetchone()['keywords_id']
            keyword_map[keyword_lower] = new_keyword_id

    return keyword_map


def keyword_matching(cleaned_bluesky_data: pd.DataFrame, keyword_map: dict) -> pd.DataFrame:
    """Assign keyword_id to rows in the DataFrame based on matching keywords in content."""

    cleaned_bluesky_data['keyword_id'] = None

    # Loop through the keywords and assign keyword_id to matching rows
    for keyword, keyword_id in keyword_map.items():
        mask = cleaned_bluesky_data['keyword'].str.contains(
            rf'\b{keyword}\b', case=False, na=False)
        cleaned_bluesky_data.loc[mask, 'keyword_id'] = keyword_id

    return cleaned_bluesky_data


def add_sentiment_scores(bluesky_data: pd.DataFrame) -> pd.DataFrame:
    """Find and add the sentiment scores of each message."""
    analyzer = SentimentIntensityAnalyzer()

    bluesky_data['sentiment_score'] = bluesky_data['text'].apply(
        lambda text: analyzer.polarity_scores(text)['compound'])

    return bluesky_data


def extract_keywords_from_csv(csv_file):
    """Extracts keywords from csv file"""
    bluesky_data = pd.read_csv(csv_file)

    return bluesky_data['keyword'].unique()


if __name__ == "__main__":
    config = dotenv_values(".env")

    logging.info("Connecting to the trends RDS")
    cursor, conn = get_cursor(config)
    logging.info("Loading raw data from test_content_data.csv")
    content_dataframe = pd.read_csv(
        "bluesky_output_data/bluesky_output_20241203_110911.csv")
    keywords = extract_keywords_from_csv(
        "bluesky_output_data/bluesky_output_20241203_110911.csv")

    cleaned_dataframe = clean_data(content_dataframe, keywords)
    keyword_map = ensure_keywords_in_db(keywords, cursor, conn)
    matched_dataframe = keyword_matching(cleaned_dataframe, keyword_map)
    final_dataframe = add_sentiment_scores(matched_dataframe)

    print(final_dataframe)
