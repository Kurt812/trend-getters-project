import pandas as pd
from extract import main as emain
from transform import main as tmain

def main(topic: list[str]) -> pd.DataFrame:
    extract_df = emain(topic)
    transform_df, count = tmain(extract_df)
    return transform_df, count

if __name__ == "__main__":
    topic = ['island','darkness']
    df, count = main(topic)
    print(df)
    print(count)