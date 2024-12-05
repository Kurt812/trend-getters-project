import pandas as pd
from extract import main as emain
from transform import main as tmain

def main(topic: list[str]) -> pd.DataFrame:
    extract_df, mentions_per_hour = emain(topic)
    transform_df = tmain(extract_df)
    return transform_df, mentions_per_hour

if __name__ == "__main__":
    topic = ['island','darkness']
    df, hourly_statistics = main(topic)
    print(df)
    print(hourly_statistics)