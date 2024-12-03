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

logging.basicConfig(level=logging.INFO)


def clean_data(df: pd.DataFrame, keywords: list[str]) -> pd.DataFrame:
    """Removes any rows that don't include the keywords and removes duplicate rows."""
    filtered_df = df[df['content'].str.contains(
        r'\b(?:' + '|'.join(keywords) + r')\b', case=False, na=False, regex=True)]

    filtered_df = filtered_df.drop_duplicates()

    return filtered_df


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
def keyword_matching(df: pd.DataFrame, keyword_map: dict) -> pd.DataFrame:
    """
    Assign keyword_id to rows in the DataFrame based on matching keywords in content.
    """

    df['keyword_id'] = None

    # Loop through the keywords and assign keyword_id to matching rows
    for keyword, keyword_id in keyword_map.items():
        mask = df['content'].str.contains(
            rf'\b{keyword}\b', case=False, na=False)
        df.loc[mask, 'keyword_id'] = keyword_id

    return df


def add_sentiment_scores(df: pd.DataFrame) -> pd.DataFrame:
    """Find and add the sentiment scores of each message."""
    analyzer = SentimentIntensityAnalyzer()

    df['sentiment_score'] = df['content'].apply(
        lambda text: analyzer.polarity_scores(text)['compound'])

    return df


def get_cursor(config):
    """Importing the data into the database"""
    conn = psycopg2.connect(
        user=config["DATABASE_USERNAME"],
        password=config["DATABASE_PASSWORD"],
        host=config["DATABASE_IP"],
        port=config["DATABASE_PORT"],
        database=config["DATABASE_NAME"]
    )
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    return cursor, conn


if __name__ == "__main__":
    config = dotenv_values(".env")
    print(config)
    logging.info("Connecting to the trends RDS")
    cursor, conn = get_cursor(config)
    logging.info("Loading raw data from test_content_data.csv")
    content_dataframe = pd.read_csv("data/test_content_data.csv")
    keywords = ["crypto", "space", "AI"]
    print(content_dataframe.shape)
    cleaned_dataframe = clean_data(content_dataframe, keywords)
    keyword_map = ensure_keywords_in_db(keywords, cursor, conn)
    matched_dataframe = keyword_matching(cleaned_dataframe, keyword_map)
    final_dataframe = add_sentiment_scores(matched_dataframe)
    print(final_dataframe)
