import pandas as pd
from extract import main as emain
from transform import main as tmain
from load import main as lmain

def main(topic: list[str]) -> pd.DataFrame:
    mentions_per_hour = emain(topic)
    transform_df = tmain(mentions_per_hour)
    lmain(topic, transform_df)
    return transform_df

if __name__ == "__main__":
    topic = ['clouds','strawberries']
    main(topic)