"""A script to run an ETL pipeline"""

from extract import main as extract_main
from transform import main as transform_main
from load import main as load_main


def main(topic: list[str]) -> None:
    """Runs pipeline through extract, transform and load"""
    mentions_per_hour = extract_main(topic)
    transform_df = transform_main(mentions_per_hour)
    load_main(topic, transform_df)


if __name__ == "__main__":
    topic = ['hi']
    main(topic)
