"""Queries for dashboard"""
from os import environ as ENV
import pandas as pd
import psycopg2
from psycopg2.extensions import cursor
from psycopg2.extras import RealDictCursor, RealDictRow


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


def get_mentions_avg_sentiment_for_keyword(keyword: str, cursor: cursor) -> pd.DataFrame:
    """Query to return dataframe of data (total mentions, sentiment for each hour) corresponding to word."""
    query = """
        SELECT k.keyword, kr.total_mentions, kr.avg_sentiment, kr.date_and_hour
        FROM keyword_recordings as kr
        JOIN keywords as k ON k.keywords_id = kr.keywords_id
        WHERE keyword =%s;
        """
    cursor.execute(query, (keyword,))
    result = cursor.fetchall()
    return pd.DataFrame(result)


def get_overall_change_in_sentiment_mentions(keyword: list, cursor: cursor) -> pd.DataFrame:
    """Query to return the overall change in average sentiment of given keywords over the last 24hrs."""

    query = """
        WITH avg_sentiment_24_ago AS (
            SELECT DISTINCT ON (k.keyword) 
                kr.avg_sentiment AS avg_sentiment_24_ago, 
                kr.total_mentions AS total_mentions_24_ago, 
                k.keyword
            FROM keyword_recordings AS kr
            JOIN keywords AS k ON kr.keywords_id = k.keywords_id
            WHERE k.keyword = ANY (%s) AND date_and_hour <= NOW() - INTERVAL '24 HOURS'
            ORDER BY k.keyword, date_and_hour DESC
        ),
        avg_sentiment_now AS (
            SELECT DISTINCT ON (k.keyword)
                kr.avg_sentiment AS avg_sentiment_now, 
                kr.total_mentions AS total_mentions_now, 
                k.keyword
            FROM keyword_recordings AS kr
            JOIN keywords AS k ON kr.keywords_id = k.keywords_id
            WHERE k.keyword = ANY (%s) AND date_and_hour <= NOW()
            ORDER BY k.keyword, date_and_hour DESC
        )
        SELECT *
        FROM avg_sentiment_24_ago AS a
        JOIN avg_sentiment_now AS n ON n.keyword = a.keyword;
    """
    cursor.execute(query, (keyword, ))
    result = cursor.fetchall()

    df = pd.DataFrame(result, columns=[
        'avg_sentiment_24_ago', 'total_mentions_24_ago', 'keyword',
        'avg_sentiment_now', 'total_mentions_now'
    ])

    df['percentage_change_mentions'] = (
        (df['total_mentions_now'] - df['total_mentions_24_ago']) /
        df['total_mentions_24_ago']
    ) * 100
    df['percentage_change_avg_sentiment'] = (
        (df['avg_sentiment_now'] - df['avg_sentiment_24_ago']) /
        df['avg_sentiment_24_ago']
    ) * 100

    return df


def get_related_words(keyword: str, cursor: cursor) -> RealDictRow:
    """Query to get related words."""
    query = """
            SELECT DISTINCT k.keyword, rt.related_term
            FROM keyword_recordings AS kr
            JOIN keywords AS k ON kr.keywords_id = k.keywords_id
            JOIN related_term_assignment AS rta ON k.keywords_id = rta.keywords_id
            JOIN related_terms AS rt ON rta.related_term_id = rt.related_term_id
            WHERE k.keyword = %s
            LIMIT 5;
            """
    cursor.execute(query, (keyword, ))
    result = cursor.fetchall()
    return result


def get_keyword_id(keyword: str, cursor: cursor) -> int:
    """Given a keyword, get the keyword id."""
    query = """
            SELECT keywords_id
            FROM keywords
            WHERE keyword = %s;"""
    cursor.execute(query, (keyword, ))
    result = cursor.fetchone()
    return result.get('keywords_id')


def get_most_mentioned_word(cursor: cursor) -> RealDictRow:
    """Function returns most mentioned word (highest total_mentions)."""
    query = """
    SELECT k.keyword, SUM(kr.total_mentions) AS total_mentions, AVG(kr.avg_sentiment)
    FROM keyword_recordings AS kr
    JOIN keywords AS k ON k.keywords_id = kr.keywords_id
    WHERE kr.date_and_hour >= NOW() - INTERVAL '24 HOURS'
    GROUP BY k.keyword
    ORDER BY total_mentions DESC
    LIMIT 1;
        """
    cursor.execute(query)
    return cursor.fetchall()


def get_most_positive_word(cursor: cursor) -> RealDictRow:
    """Returns the word with the highest sentiment score over the past 24 hours, i.e. closest to 1."""
    query = """
        SELECT k.keyword, MAX(kr.avg_sentiment) AS max_sentiment, kr.date_and_hour
        FROM keyword_recordings AS kr
        JOIN keywords AS k ON k.keywords_id = kr.keywords_id
        WHERE kr.date_and_hour >= NOW() - INTERVAL '24 HOURS'
        GROUP BY k.keyword, kr.date_and_hour
        ORDER BY max_sentiment DESC
        LIMIT 1;
        """
    cursor.execute(query)
    result = cursor.fetchall()
    return result


def get_most_negative_word(cursor: cursor) -> RealDictRow:
    """Returns the word with the lowest sentiment score over the past 24 hours, i.e. closest to -1."""
    query = """
        SELECT k.keyword, MIN(kr.avg_sentiment) AS min_sentiment, kr.date_and_hour
        FROM keyword_recordings AS kr
        JOIN keywords AS k ON k.keywords_id = kr.keywords_id
        WHERE kr.date_and_hour >= NOW() - INTERVAL '24 HOURS'
        GROUP BY k.keyword, kr.date_and_hour
        ORDER BY min_sentiment ASC
        LIMIT 1;
        """
    cursor.execute(query)
    return cursor.fetchall()
