import pandas as pd
from pytrends.request import TrendReq
from df_bluesky_extract import main as bs_main
from trends_extract import initialize_trend_request, fetch_suggestions
from transform import main as tf_main


def main(topics: list[str]) -> pd.DataFrame:
    """"""
    extract_dataframe = bs_main(topics)

    pytrend = initialize_trend_request()
    extract_dataframe['related_terms'] = ""

    for keyword in topics:
        keyword_suggestions = fetch_suggestions(pytrend, keyword)
        related_terms = [suggestion['title']
                         for suggestion in keyword_suggestions]
        related_keywords = ",".join(related_terms)

        extract_dataframe.loc[extract_dataframe['keyword']
                              == keyword, 'related_terms'] = related_keywords

    transformed_dataframe = tf_main(extract_dataframe)
    return transformed_dataframe


if __name__ == "__main__":
    topics = ['sky', 'sea']
    print(main(topics))
