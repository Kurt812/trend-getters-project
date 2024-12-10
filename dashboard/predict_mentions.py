"""Predicts the total mentions for the next hour for a given keyword"""

import logging
from os import environ as ENV
import pandas as pd
import numpy as np
import psycopg2
import psycopg2.extras
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import StandardScaler
from dotenv import load_dotenv

def setup_connection() -> psycopg2.extensions.cursor:
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
        logging.error("Operational error while connecting to the database: %s", e)
        raise
    except Exception as e:
        logging.error("Error connecting to database: %s", e)
        raise
    logging.info("Connection successfully established to database.")
    return curs


def extract_keywords_recordings_data(curs: psycopg2.extensions.cursor,
                                     keyword: str) -> pd.DataFrame:
    """Extracts the keyword recordings"""
    try:
        curs.execute("SELECT keywords_id FROM keywords WHERE keyword = %s", (keyword,))
        keyword_id = curs.fetchone()['keywords_id']
        curs.execute("""SELECT * FROM keyword_recordings
                     WHERE keywords_id = %s
                     ORDER BY date_and_hour ASC""", (keyword_id,))
        results = curs.fetchall()
        return pd.DataFrame(results)
    except ValueError:
        return "Keyword is not present in the database"


def data_transformation(keyword_df: pd.DataFrame) -> pd.DataFrame:
    """Creating necessary information in the dataframe for the model"""
    keyword_df['date_and_hour'] = pd.to_datetime(keyword_df['date_and_hour'])
    keyword_df = keyword_df.sort_values(by='date_and_hour')


    keyword_df['hour_of_day'] = keyword_df['date_and_hour'].dt.hour
    keyword_df['day_of_week'] = keyword_df['date_and_hour'].dt.dayofweek


    keyword_df['Mentions from 1 hour ago'] = keyword_df['total_mentions'].shift(1)
    keyword_df['Mentions from 2 hours ago'] = keyword_df['total_mentions'].shift(2)
    keyword_df['Average of last 3 hours'] = keyword_df['total_mentions'].rolling(window=3).mean()

    keyword_df = keyword_df.dropna() # Getting rid of first two hours
    # because we don't want NaNs when training

    return keyword_df


def train_model(keyword_dataframe: pd.DataFrame) -> tuple:
    """Train a model to predict total_mentions"""
    input_columns = ['hour_of_day', 'day_of_week', 'Mentions from 1 hour ago',
                     'Mentions from 2 hours ago', 'Average of last 3 hours']
    filtered_dataframe = keyword_dataframe[input_columns]
    mentions_from_dataframe = keyword_dataframe['total_mentions']


    scaler = StandardScaler()
    scaled_dataframe = scaler.fit_transform(filtered_dataframe)
    # Ensures that inputs with different scales
    # e.g. hour_of_day and total_mentions
    # don't disproportinately affect the model (Normalises)

    training_inp, training_out, testing_inp, testing_out = train_test_split(scaled_dataframe,
                                                        mentions_from_dataframe,
                                                        test_size=0.2,
                                                        random_state=42)


    model = RandomForestRegressor(n_estimators=100, random_state=42)
    model.fit(training_inp, testing_inp)

    return model, scaler, input_columns


def predict_next_hour(model: RandomForestRegressor, scaler: StandardScaler,
                      input_columns: list[str], most_recent_data: pd.DataFrame) -> np.ndarray:
    """Predict total mentions for the next hour"""
    most_recent_data_key_metrics = most_recent_data[input_columns]

    most_recent_data_scaled = scaler.transform(most_recent_data_key_metrics)
    # Ensures scaling for columns with larger numerical values

    next_hour_prediction = model.predict(most_recent_data_scaled)

    return next_hour_prediction


def main(keyword: str) -> float:
    """Runs functions to train a model to predict the total mentions for the next hour"""
    load_dotenv()
    curs = setup_connection()


    keyword_dataframe = extract_keywords_recordings_data(curs, keyword)

    enriched_dataframe = data_transformation(keyword_dataframe)


    model, scaler, input_columns = train_model(enriched_dataframe)

    most_recent_data = enriched_dataframe.tail(1)
    prediction = predict_next_hour(model, scaler, input_columns, most_recent_data)

    return prediction[0]

if __name__ == "__main__":
    keyword_for_prediction = "hi"
    main(keyword_for_prediction)
    print(main(keyword_for_prediction))
