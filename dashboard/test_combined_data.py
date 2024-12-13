"""Test script for combined_data.py"""

import logging
from unittest.mock import patch, MagicMock
import pandas as pd
import pytest
import psycopg2
from combined_data import (
    get_connection, download_csv_from_s3_to_dataframe, fetch_keyword_recordings_as_dataframe, main_combine)


@pytest.fixture
def aws_env_vars():
    """Patched environment variables."""
    with patch.dict("os.environ", {
        "DB_HOST": "fake_host",
        "DB_PORT": "1234",
        "DB_NAME": "fake_db",
        "DB_USERNAME": "fake_user",
        "DB_PASSWORD": "password",
        "SCHEMA_NAME": "schema",
        "S3_BUCKET_NAME": "bucket_name",
        "S3_FOLDER_NAME": "folder_name",
        "S3_FILE_NAME": "file_name"

    }):
        yield


@patch('combined_data.psycopg2.connect')
def test_successful_s3_connection(mock_connect, aws_env_vars, caplog):
    """Test the successful connection to an S3 client without real-world side effects."""
    with caplog.at_level(logging.INFO):
        get_connection()
    mock_connect.assert_called_once_with(
        host="fake_host", port="1234", database="fake_db", user="fake_user", password="password")
    assert 'Connection successfully established to database.' in caplog.text


@patch('combined_data.psycopg2.connect')
def test_postgres_connection_operational_error(mock_connect, aws_env_vars, caplog):
    """Test unsuccessful connection to PostgreSQL due to OperationalError"""

    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn
    mock_connect.side_effect = psycopg2.OperationalError()

    with pytest.raises(psycopg2.OperationalError):
        get_connection()
    assert 'Operational error while connecting to the database:' in caplog.text
    for record in caplog.records:
        assert record.levelname == 'ERROR'


@patch('combined_data.psycopg2.connect')
def test_postgres_connection_exception(mock_connect, aws_env_vars, caplog):
    """Test general Exception raised when error setting up connection"""

    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn
    mock_connect.side_effect = Exception()

    with pytest.raises(Exception):
        get_connection()
    assert 'Error connecting to database:' in caplog.text
    for record in caplog.records:
        assert record.levelname == 'ERROR'


@patch('combined_data.os.remove')
@patch('combined_data.pd.read_csv')
@patch('os.getcwd', return_value='/mock/directory')
@patch('combined_data.boto3.client')
def test_successful_download_csv(mock_client, mock_getcwd, mock_readcsv, mock_remove):
    """Test the successful download of a csv from S3 bucket to a data frame."""
    mock_client.download_file.return_value = None
    mock_readcsv.return_value = pd.DataFrame(
        {"date_and_hour": ["2024-12-12 10:00:00"]})
    mock_s3 = MagicMock()
    mock_client.return_value = mock_s3

    bucket_name = "test-bucket"
    folder_name = "test-folder"
    file_name = "temp_file.csv"
    df = download_csv_from_s3_to_dataframe(
        bucket_name, folder_name, file_name)

    mock_client.assert_called_once_with("s3")
    mock_s3.download_file.assert_called_once_with(
        bucket_name, f"{folder_name}/{file_name}", "/mock/directory/temp_file.csv")
    mock_readcsv.assert_called_once_with("/mock/directory/temp_file.csv")
    mock_remove.assert_called_once_with("/mock/directory/temp_file.csv")
    assert isinstance(df, pd.DataFrame)
    assert "date_and_hour" in df.columns


@patch('combined_data.os.remove')
@patch('combined_data.pd.read_csv')
@patch('os.getcwd', return_value='/mock/directory')
@patch('combined_data.boto3.client')
def test_download_csv_exception(mock_client, mock_getcwd, mock_readcsv, mock_remove, caplog):
    """Test case when something goes wrong in the downloading of csv from the S3 or the conversion to dataframe, an error is raised."""

    mock_s3 = MagicMock()
    mock_client.return_value = mock_s3
    mock_client.download_file.side_effect = Exception()

    bucket_name = "test-bucket"
    folder_name = "test-folder"
    file_name = "temp_file.csv"
    with caplog.at_level(logging.INFO):
        df = download_csv_from_s3_to_dataframe(
            bucket_name, folder_name, file_name)

    assert df is None
    assert "An error occurred:" in caplog.text


@patch('combined_data.pd.read_sql_query')
@patch('combined_data.get_connection')
def test_fetch_keyword_recordings_success(mock_conn, mock_read_sql, aws_env_vars):
    """Test successful return of dataframe version of keyword recording table."""
    mock_s3 = MagicMock()
    mock_conn.return_value = mock_s3

    mock_read_sql.return_value = pd.DataFrame({
        'keyword_recordings_id': [1],
        'keywords_id': [10],
        'total_mentions': [100],
        'avg_sentiment': [0.5],
        'date_and_hour': ['2024-12-12 12:00:00']
    })

    df = fetch_keyword_recordings_as_dataframe()

    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    mock_conn.assert_called_once()
    mock_read_sql.assert_called_once()
    mock_s3.close.assert_called_once()


@patch('combined_data.pd.read_sql_query')
@patch('combined_data.get_connection')
def test_fetch_keyword_recordings_error(mock_conn, mock_read_sql, aws_env_vars):
    """Test fetching keywords returns an error if needed."""
    mock_s3 = MagicMock()
    mock_conn.return_value = mock_s3
    mock_conn.side_effect = psycopg2.Error

    df = fetch_keyword_recordings_as_dataframe()

    assert df is None
    mock_conn.assert_called_once()
    mock_read_sql.assert_not_called()


@patch("combined_data.fetch_keyword_recordings_as_dataframe")
@patch("combined_data.download_csv_from_s3_to_dataframe")
@patch('combined_data.get_connection')
def test_main_combine_success(mock_conn, mock_download, mock_fetch_df, aws_env_vars, caplog):
    """Test the main combine function successful downloads a csv file and returns a dataframe."""
    mock_download.return_value = pd.DataFrame({
        'keyword_recordings_id': [1],
        'keywords_id': [10],
        'total_mentions': [100],
        'avg_sentiment': [0.5],
        'date_and_hour': ['2024-12-12 12:00:00']
    })
    mock_fetch_df.return_value = pd.DataFrame({
        'keyword_recordings_id': [2],
        'keywords_id': [20],
        'total_mentions': [10],
        'avg_sentiment': [-.1],
        'date_and_hour': ['2024-12-12 06:00:00']
    })

    with caplog.at_level(logging.INFO):
        result = main_combine()

    assert isinstance(result, pd.DataFrame)
    assert not result.empty
    assert len(result) == 2
    mock_download.assert_called_once()
    mock_fetch_df.assert_called_once()
    assert 'Combined DataFrame from S3 created successfully.' in caplog.text


@patch("combined_data.fetch_keyword_recordings_as_dataframe")
@patch("combined_data.download_csv_from_s3_to_dataframe")
@patch('combined_data.get_connection')
def test_main_combine_one_df_none_error(mock_conn, mock_download, mock_fetch_df, aws_env_vars, caplog):
    """Test error is logged if one of the dfs are returned as none."""
    mock_download.return_value = None
    mock_fetch_df.return_value = pd.DataFrame({
        'keyword_recordings_id': [1],
        'keywords_id': [10],
        'total_mentions': [100],
        'avg_sentiment': [0.5],
        'date_and_hour': ['2024-12-12 12:00:00']
    })

    with caplog.at_level(logging.INFO):
        result = main_combine()

    assert result is None
    assert 'Could not combine DataFrames due to missing data.' in caplog.text
    mock_download.assert_called_once()
    mock_fetch_df.assert_called_once()
