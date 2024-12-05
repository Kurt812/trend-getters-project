import pandas as pd
from extract import main as emain
from transform import main as tmain

def main(topic: list[str]) -> pd.DataFrame:
    mentions_per_hour = emain(topic)
    transform_df = tmain(mentions_per_hour)
    return transform_df

if __name__ == "__main__":
    topic = ['island','darkness']
    hourly_statistics = main(topic)
    print(hourly_statistics)