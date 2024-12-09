"""Predicts the mentions for a given keyword for the next hour"""

from json import load
import pandas as pd
import logging
import numpy as np
from os import environ as ENV
import psycopg2
import psycopg2.extras
from psycopg2.extensions import cursor
from dotenv import load_dotenv
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, classification_report

def setup_connection() -> cursor:
    """Retrieve database connection and cursor"""
    try:
        conn = psycopg2.connect(
            user=ENV["DB_USERNAME"],
            password=ENV["DB_PASSWORD"],
            host=ENV["DB_HOST"],
            port=ENV["DB_PORT"],
            database=ENV["DB_NAME"]
        )
        curs = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        curs.execute(f"SET SEARCH_PATH TO {ENV['SCHEMA_NAME']};")
    except psycopg2.OperationalError as e:
        logging.error(
            "Operational error while connecting to the database: %s", e)
        raise
    except Exception as e:
        logging.error("Error connecting to database: %s", e)
        raise
    logging.info("Connection successfully established to database.")

    return curs

def extract_keywords_recordings_data(curs: cursor, keyword: str) -> pd.DataFrame:
    """Extracts the keyword recordings"""
    curs.execute("SELECT keywords_id FROM keywords WHERE keyword = %s", (keyword, ))
    keyword_id = curs.fetchone()['keywords_id']

    curs.execute("SELECT * FROM keyword_recordings WHERE keywords_id = %s", (keyword_id, ))
    results = curs.fetchall()
    return pd.DataFrame(results)


def create_labels(keyword_dataframe: pd.DataFrame) -> np.ndarray:
    """Creates labels from the mentions count """
    return keyword_dataframe['total_mentions'].values


def split_dataset(keyword_dataframe: np.ndarray, labels: np.ndarray):
    """Splits the dataset into training and testing datasets"""
    train_review_vectors, test_review_vectors, train_labels, test_labels = train_test_split(
        keyword_dataframe,
        labels,
        test_size=0.2,
        random_state=42,
    )
    return train_review_vectors, test_review_vectors, train_labels, test_labels


def train_model(train_review_vectors, train_labels):
    """Train a machine learning model using logistice regression"""
    model = LogisticRegression(max_iter=1000, random_state=42)
    model.fit(train_review_vectors, train_labels)
    return model


def evaluate_model(model, test_review_vectors, test_labels):
    """Evaluate the machine learning model using accuracy and a classification report"""
    predicted_labels = model.predict(test_review_vectors)
    accuracy = accuracy_score(test_labels, predicted_labels)
    return accuracy, classification_report(test_labels, predicted_labels)



def main(keyword: str):
    """Runs a language model to calculate a prediction for the mentions count"""
    load_dotenv()
    curs = setup_connection()

    keyword_dataframe = extract_keywords_recordings_data(curs, keyword)
    print(keyword_dataframe)
    return
    labels = create_labels(keyword_dataframe)

    train_review_vectors, test_review_vectors, train_labels, test_labels = split_dataset(
        keyword_dataframe, labels)

    model = train_model(train_review_vectors, train_labels)

    accuracy, report = evaluate_model(model, test_review_vectors, test_labels)
    print(f'Accuracy of the machine leaning model: {accuracy}')
    print(f'Machine learning model report: \n {report}')


if __name__ == "__main__":
    keyword = "chocolate"
    main(keyword)