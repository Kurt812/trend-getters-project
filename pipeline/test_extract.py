"""Test script for extract python file."""
# pylint: skip-file

import os
import logging
import json
from unittest.mock import patch, MagicMock, ANY
from datetime import datetime
from io import BytesIO
import pandas as pd
import pandas.testing as pdt
import pytest
from botocore.config import Config
from extract import (s3_connection, average_sentiment_analysis,
                     extract_s3_data, initialize_trend_request, fetch_suggestions, main)


@pytest.fixture
def aws_env_vars():
    """Patched environment variables."""
    with patch.dict("os.environ", {"AWS_ACCESS_KEY_ID": "fake_access_key", "AWS_SECRET_ACCESS_KEY": "fake_secret_key", "S3_BUCKET_NAME": "bucket_name"}):
        yield


@pytest.fixture
def file_data():
    """Fixture that provides a common data dictionary for sentiment analysis tests."""
    return {
        'python is great': {'Sentiment Score': {'compound': 0.5}},
        'python coding': {'Sentiment Score': {'compound': 0.7}},
        'python not good': {'Sentiment Score': {'compound': -0.3}},
    }


@patch('extract.client')
def test_successful_s3_connection(mock_client, aws_env_vars):
    """Test the successful connection to an S3 client without real-world side effects."""
    mock_config = Config(
        connect_timeout=5,
        read_timeout=10,
        retries={
            'max_attempts': 3,
            'mode': 'standard'
        },
        max_pool_connections=110
    )

    s3_connection()
    mock_client.assert_called_once_with(
        's3',
        aws_access_key_id='fake_access_key',
        aws_secret_access_key='fake_secret_key',
        config=ANY
    )

    actual_call = mock_client.call_args
    actual_config = actual_call.kwargs['config']

    assert actual_config.connect_timeout == mock_config.connect_timeout
    assert actual_config.read_timeout == mock_config.read_timeout
    assert actual_config.retries == mock_config.retries
    assert actual_config.max_pool_connections == mock_config.max_pool_connections


@patch.dict(os.environ, {}, clear=True)
@patch('extract.client')
def test_unsuccessful_s3_connection_missing_env(mock_client, caplog):
    """Test that missing env variables will be handled gracefully. """
    mock_client.return_value = None
    with caplog.at_level(logging.INFO):
        result = s3_connection()
    assert result is None
    mock_client.assert_called_once()
    assert 'Missing required AWS credentials in .env file' in caplog.text


@patch('extract.client')
def test_s3_connection_connection_error(mock_client, caplog):
    """Test that missing env variables will be handled gracefully. """
    mock_client.side_effect = ConnectionError("Connection Error")

    with caplog.at_level(logging.INFO):
        result = s3_connection()
    assert result is None
    mock_client.assert_called_once()
    assert 'An error occurred attempting to connect to S3:' in caplog.text


@patch('extract.TrendReq')
def test_initialize_trend_success(mock_trendreq):
    """Test successful initialisation of trend request."""
    mock_trendreq_instance = MagicMock()
    mock_trendreq.return_value = mock_trendreq_instance

    initialize_trend_request()

    mock_trendreq.assert_called_once()


def test_average_sentiment_analysis_non_zero_mentions(file_data):
    """Test mentions non-zero case"""

    keyword = 'python'

    avg_sentiment, mentions = average_sentiment_analysis(keyword, file_data)

    assert avg_sentiment == 0.3
    assert mentions == 3


def test_average_sentiment_analysis_no_mentions(file_data):
    """Test case for no mentions of keyword found"""
    keyword = 'sky'

    avg_sentiment, mentions = average_sentiment_analysis(keyword, file_data)

    assert avg_sentiment == 0
    assert mentions == 0


def test_average_sentiment_analysis_file_empty():
    """Test case for when the file being searched is empty"""
    keyword = 'sky'
    file_data = {}

    avg_sentiment, mentions = average_sentiment_analysis(keyword, file_data)
    assert avg_sentiment == 0
    assert mentions == 0


@patch('datetime.datetime')
@patch('extract.client')
def test_extract_s3_success(mock_client, mock_datetime):
    """Test successful extraction of data from s3 into pd.dataframe."""
    mock_datetime.now.return_value = datetime(2024, 12, 9)
    mock_datetime.strftime = datetime.strftime
    bucket_name = 'bucket_name'
    topics = ['python']

    mock_list_objects_response = {
        'Contents': [
            {'Key': 'bluesky/2024-12-09/00.json',
                'LastModified': datetime(2024, 12, 9, 1, 0, 1), 'Size': 1324861},
            {'Key': 'bluesky/2024-12-09/01.json',
                'LastModified': datetime(2024, 12, 9, 2, 0, 1), 'Size': 1411986},
        ]
    }
    mock_client.list_objects_v2.return_value = mock_list_objects_response

    mock_json_content_1 = {
        'python is great': {'Sentiment Score': {'compound': 0.5}},
        'python coding': {'Sentiment Score': {'compound': 0.7}}
    }

    mock_json_content_2 = {
        'python not good': {'Sentiment Score': {'compound': -0.3}},
        'python bad': {'Sentiment Score': {'compound': -0.6}}
    }

    mock_client.get_object.side_effect = [
        {'Body': BytesIO(json.dumps(mock_json_content_1).encode('utf-8'))},
        {'Body':  BytesIO(json.dumps(mock_json_content_2).encode('utf-8'))}
    ]

    result = extract_s3_data(mock_client, bucket_name, topics)
    assert not result.empty
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 2
    assert 'Keyword' in result.columns
    assert 'Average Sentiment' in result.columns
    assert 'Total Mentions' in result.columns
    assert 'Hour' in result.columns


@patch('datetime.datetime')
@patch('extract.client')
def test_extract_s3_no_files(mock_client, mock_datetime, caplog):
    """Test when no files are found, ValueError is raised."""
    mock_datetime.now.return_value = datetime(2024, 12, 9)
    mock_datetime.strftime = datetime.strftime
    bucket_name = 'bucket_name'
    topics = ['python']

    mock_client.list_objects_v2.return_value = {}
    with caplog.at_level(logging.INFO):
        with pytest.raises(ValueError):
            extract_s3_data(mock_client, bucket_name, topics)


@patch('extract.fetch_suggestions')
@patch('extract.initialize_trend_request')
@patch('extract.extract_s3_data')
@patch('extract.s3_connection')
def test_main_success(mock_s3_conn, mock_extract_s3, mock_pytrend, mock_suggestions, aws_env_vars):
    """Test main function is successfully run."""
    mock_s3 = MagicMock()
    mock_s3_conn.return_value = mock_s3
    topic = ['python']
    mock_data = [
        {"Hour": "00", "Keyword": "python",
            "Average Sentiment": 0.6, "Total Mentions": 2},
        {"Hour": "01", "Keyword": "python",
            "Average Sentiment": -0.3, "Total Mentions": 2},
    ]
    extracted_df = pd.DataFrame(mock_data)
    mock_extract_s3.return_value = extracted_df
    mock_suggestions.return_value = [
        {'title': 'python tutorial'}, {'title': 'python programming'}]

    expected_data = [
        {'Hour': '00', 'Keyword': "python", "Average Sentiment": 0.6, "Total Mentions": 2,
         "Related Terms": "python tutorial,python programming"},
        {"Hour": "01", "Keyword": "python", "Average Sentiment": -0.3, "Total Mentions": 2,
         "Related Terms": "python tutorial,python programming"}
    ]
    expected_df = pd.DataFrame(expected_data)

    result = main(topic)
    assert isinstance(result, pd.DataFrame)
    expected_columns = {'Hour', 'Keyword',
                        'Average Sentiment', 'Total Mentions', 'Related Terms'}

    assert set(result.columns) == expected_columns
    mock_extract_s3.assert_called_once_with(mock_s3, 'bucket_name', ['python'])
    mock_pytrend.assert_called_once()
    pdt.assert_frame_equal(result, expected_df)


@patch('extract.TrendReq')
def test_fetch_suggestions(mock_trendreq):
    """Test function to ensure related words are returned successfully."""

    mock_pytrend_instance = MagicMock()
    keyword = 'good'

    mock_related_words = [
        {'mid': 'i', 'title': 'Goodfellas', 'type': '1990 film'},
        {'mid': 'not', 'title': 'Goodyear', 'type': 'Topic'},
        {'mid': 'sure', 'title': 'The Good Doctor', 'type': 'Drama series'},
        {'mid': 'yes', 'title': 'DICK’S Sporting Goods', 'type': 'Topic'},
        {'mid': 'yes', 'title': 'Goodles', 'type': 'Topic'}]
    mock_pytrend_instance.suggestions.return_value = mock_related_words
    mock_trendreq.return_value = mock_pytrend_instance
    result = fetch_suggestions(mock_pytrend_instance, keyword)
    assert result == mock_related_words
