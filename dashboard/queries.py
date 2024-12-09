"""Queries for dashboard"""

import pandas as pd
from psycopg2.extensions import cursor

def get_mentions_avg_sentiment_for_keyword(keyword: str, cursor: cursor) -> pd.DataFrame:
    """Query to return dataframe of data corresponding to word"""
    query = """
        SELECT k.keyword, kr.total_mentions, kr.avg_sentiment, kr.date_and_hour
        FROM keyword_recordings as kr 
        JOIN keywords as k ON k.keywords_id = kr.keywords_id
        WHERE keyword =%s;  
        """
    cursor.execute(query, (keyword,))
    result = cursor.fetchall()
    return pd.DataFrame(result)

def get_overall_change_in_sentiment_mentions(keyword: str, cursor: cursor) -> pd.DataFrame:
    """Query to return the overall change in average sentiment of keyword given over last 24hrs."""

    query = """
        WITH avg_sentiment_24_ago AS (
            SELECT kr.avg_sentiment as avg_sentiment_24_ago, kr.total_mentions as total_mentions_24_ago, k.keyword
            FROM keyword_recordings as kr
            JOIN keywords as k ON kr.keywords_id = k.keywords_id
            WHERE k.keyword = %s AND date_and_hour <= NOW()- INTERVAL '24 HOURS'
            ORDER BY date_and_hour DESC
            LIMIT 1
            ),
        avg_sentiment_now AS (
            SELECT kr.avg_sentiment as avg_sentiment_now, kr.total_mentions as total_mentions_now, k.keyword
            FROM keyword_recordings as kr
            JOIN keywords as k ON kr.keywords_id = k.keywords_id
            WHERE k.keyword = %s AND date_and_hour <= NOW()
            ORDER BY date_and_hour DESC
            LIMIT 1
            )
        SELECT *
        
        FROM avg_sentiment_24_ago as a
        JOIN avg_sentiment_now as n ON n.keyword = a.keyword;
        """
    cursor.execute(query, (keyword, keyword))
    result = cursor.fetchall()

    df = pd.DataFrame(result)
    df['percentage_change_mentions'] = ((df['total_mentions_now']-df['total_mentions_24_ago'])/df['total_mentions_24_ago'])*100
    df['percentage_change_avg_sentiment'] = ((df['avg_sentiment_now']-df['avg_sentiment_24_ago'])/df['avg_sentiment_24_ago'])*100

    return df
    
def get_related_words_stats(keyword: str, cursor: cursor) -> pd.DataFrame:
    """Query ..."""
    query = """
            SELECT kr.total_mentions, kr.avg_sentiment, k.keyword, rt.related_term 
            FROM keyword_recordings as kr
            JOIN keywords as k ON kr.keywords_id = k.keywords_id
            JOIN related_term_assignment as rta ON k.keywords_id = rta.keywords_id
            JOIN related_terms as rt ON rta.related_term_id = rt.related_term_id;"""
