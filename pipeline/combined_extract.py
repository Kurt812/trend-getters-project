import pandas as pd
from pytrends.request import TrendReq
from bluesky_extract import main as bs_main
from trends_extract import initialize_trend_request, fetch_suggestions
from transform import main as tf_main


def main(topics: list[str]) -> pd.DataFrame:
    """Extract and transform data from multiple sources"""
    extract_dataframe = bs_main(topics)

    # Debug print to understand the DataFrame
    print("DataFrame columns:", list(extract_dataframe.columns))
    print("DataFrame:\n", extract_dataframe)

    # Check if the DataFrame is empty
    if extract_dataframe.empty:
        print("No matching texts found in S3 bucket.")
        return extract_dataframe

    pytrend = initialize_trend_request()
    extract_dataframe['related_terms'] = ""

    for keyword in topics:
        # Use the correct column name (likely 'Keyword' with a capital K)
        extract_dataframe.loc[extract_dataframe['Keyword']
                              == keyword, 'related_terms'] = ",".join(
            [suggestion['title'] for suggestion in fetch_suggestions(pytrend, keyword)]
        )
    transformed_dataframe = tf_main(extract_dataframe)
    return transformed_dataframe


if __name__ == "__main__":
    topics = ['wine','alexa']
    print(main(topics))