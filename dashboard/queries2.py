"""Queries for pandas dataframe"""

import pandas as pd

from combined_data import main_combine


def get_percentage_change_mentions_sentiment(keywords: list, data: pd.DataFrame):
    """Return a pd.DataFrame with calculated changes over the last 24hrs in mention count and sentiment."""
    filtered_data = data[data['keyword'].isin(keywords)].copy()
    now = pd.Timestamp.now()
    data_24_hours_ago = now - pd.Timedelta(hours=24)

    now_data = filtered_data[filtered_data['date_and_hour'] <= now].sort_values(
        by='date_and_hour').groupby('keyword').tail(1)
    data_24_ago = filtered_data[filtered_data['date_and_hour'] <= data_24_hours_ago].sort_values(
        by='date_and_hour').groupby('keyword').tail(1)

    merged_df = pd.merge(
        now_data[['keyword', 'avg_sentiment',
                  'total_mentions', 'date_and_hour']],
        data_24_ago[['keyword', 'avg_sentiment',
                     'total_mentions', 'date_and_hour']],
        on='keyword',
        suffixes=('_now', '_24_hrs_ago')
    )
    merged_df['percentage_change_mentions'] = (
        (merged_df['total_mentions_now'] - merged_df['total_mentions_24_hrs_ago']) /
        merged_df['total_mentions_24_hrs_ago']
    ) * 100

    merged_df['percentage_change_avg_sentiment'] = (
        (merged_df['avg_sentiment_now'] - merged_df['avg_sentiment_24_hrs_ago']) /
        merged_df['avg_sentiment_24_hrs_ago']
    ) * 100
    print(merged_df.columns)

    return merged_df[['keyword', 'avg_sentiment_24_hrs_ago', 'avg_sentiment_now',
                      'total_mentions_24_hrs_ago', 'total_mentions_now',
                      'percentage_change_mentions', 'percentage_change_avg_sentiment']]


if __name__ == "__main__":
    df = main_combine()

    print(get_percentage_change_mentions_sentiment(['hello', 'stars'], df))
