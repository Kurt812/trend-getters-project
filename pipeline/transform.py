"""This script cleans the data, matches keyword ids and sentiment scores."""

import logging
import os
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
    return bluesky_data, keywords
    # filtered_bluesky = bluesky_data[bluesky_data['content'].str.contains(
    #     r'\b(?:' + '|'.join(keywords) + r')\b', case=False, na=False, regex=True)]

    # filtered_bluesky = filtered_bluesky.drop_duplicates()

    # return filtered_bluesky


def ensure_keywords_in_db(keywords: list, cursor, connection):
    """Ensure all keywords are present in the database. Add missing keywords."""

    cursor.execute("SELECT keyword, keywords_id FROM keywords")
    rows = cursor.fetchall()

    # Convert rows into a dictionary
    keyword_map = {row['keyword']: row['keywords_id'] for row in rows}

    for keyword in keywords:
        keyword_lower = keyword.lower()  # Normalize for case-insensitivity
        if keyword_lower not in keyword_map:
            cursor.execute(
                "INSERT INTO keywords (keyword) VALUES (%s)",
                (keyword_lower,)
            )
            connection.commit()
            new_keyword_id = cursor.fetchone()['keywords_id']
            keyword_map[keyword_lower] = new_keyword_id

    return keyword_map


# also want to match to topic_id?
def keyword_matching(cleaned_bluesky_data: pd.DataFrame, keyword_map: dict) -> pd.DataFrame:
    """Assign keyword_id to rows in the DataFrame based on matching keywords in content."""

    cleaned_bluesky_data['keyword_id'] = None

    # Loop through the keywords and assign keyword_id to matching rows
    for keyword, keyword_id in keyword_map.items():
        mask = cleaned_bluesky_data['content'].str.contains(
            rf'\b{keyword}\b', case=False, na=False)
        cleaned_bluesky_data.loc[mask, 'keyword_id'] = keyword_id

    return cleaned_bluesky_data


def add_sentiment_scores(df: pd.DataFrame) -> pd.DataFrame:
    """Find and add the sentiment scores of each message."""
    analyzer = SentimentIntensityAnalyzer()

    df['sentiment_score'] = df['content'].apply(
        lambda text: analyzer.polarity_scores(text)['compound'])

    return df


def extract_keywords_from_csv(csv_file):
    # Read the CSV file
    df = pd.read_csv(csv_file)

    # Extract unique keywords from the 'keyword' column
    unique_keywords = df['keyword'].unique()

    return unique_keywords


if __name__ == "__main__":
    config = dotenv_values(".env")

    logging.info("Connecting to the trends RDS")
    cursor, conn = get_cursor(config)
    logging.info("Loading raw data from test_content_data.csv")
    content_dataframe = pd.read_csv(
        "bluesky_output_data/bluesky_output_20241203_110911.csv")
    keywords = extract_keywords_from_csv(
        "bluesky_output_data/bluesky_output_20241203_110911.csv")

    print(clean_data(content_dataframe, keywords))
    # cleaned_dataframe = clean_data(content_dataframe, keywords)
    # keyword_map = ensure_keywords_in_db(keywords, cursor, conn)
    # matched_dataframe = keyword_matching(cleaned_dataframe, keyword_map)
    # final_dataframe = add_sentiment_scores(matched_dataframe)

    # print(final_dataframe)
