"""This script cleans the data, matches keyword ids and sentiment scores."""

# pylint: disable=E0401

import logging
import pandas as pd
import psycopg2
import psycopg2.extras
from psycopg2.extensions import cursor as curs, connection as conn
from dotenv import dotenv_values
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
    ]
)


def get_connection(config: dotenv_values) -> conn:
    """Importing the data into the database"""
    return psycopg2.connect(
        user=config["DB_USERNAME"],
        password=config["DB_PASSWORD"],
        host=config["DB_HOST"],
        port=config["DB_PORT"],
        database=config["DB_NAME"]
    )


def get_cursor(connection: conn) -> curs:
    """Returns the a psycopg2 cursor"""
    return connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)


def clean_data(bluesky_data: pd.DataFrame, keywords: list[str]) -> pd.DataFrame:
    """Removes any rows that don't include the keywords and removes duplicate rows."""

    filtered_bluesky = bluesky_data[bluesky_data['Text'].str.contains(
        r'\b(?:' + '|'.join(keywords) + r')\b', case=False, na=False, regex=True)]
    filtered_bluesky = bluesky_data[bluesky_data['Keyword'].notnull()]
    filtered_bluesky = bluesky_data.drop_duplicates()

    return filtered_bluesky


def ensure_keywords_in_db(keywords: list, cursor: curs, connection: conn) -> dict:
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
        mask = cleaned_bluesky_data['Keyword'].str.contains(
            rf'\b{keyword}\b', case=False, na=False)
        cleaned_bluesky_data.loc[mask, 'keyword_id'] = keyword_id

    return cleaned_bluesky_data


def add_sentiment_scores(bluesky_data: pd.DataFrame) -> pd.DataFrame:
    """Find and add the sentiment scores of each message."""
    analyzer = SentimentIntensityAnalyzer()

    bluesky_data['sentiment_score'] = bluesky_data['Text'].apply(
        lambda text: analyzer.polarity_scores(text)['compound'])

    return bluesky_data


def extract_keywords_from_csv(csv_file) -> pd.Series:
    """Extracts keywords from csv file"""
    bluesky_data = pd.read_csv(csv_file)

    return bluesky_data['Keyword'].unique()


def main(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Main function to run transform.py"""
    env_values = dotenv_values(".env")

    logging.info("Connecting to the trends RDS")
    connection = get_connection(env_values)
    cursor = get_cursor(connection)
    keywords_from_dataframe = list(dataframe['Keyword'])

    cleaned_dataframe = clean_data(dataframe, keywords_from_dataframe)
    keyword_map = ensure_keywords_in_db(
        keywords_from_dataframe, cursor, connection)
    matched_dataframe = keyword_matching(cleaned_dataframe, keyword_map)
    final_dataframe = add_sentiment_scores(matched_dataframe)

    return final_dataframe


if __name__ == "__main__":
    main()
