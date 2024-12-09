"""Test script for extract python file."""
# pylint: skip-file

import os
import logging
import json
from unittest.mock import patch, MagicMock, ANY
from datetime import datetime
import pandas as pd
import pytest
from botocore.config import Config
import boto3
from io import BytesIO
from botocore.exceptions import ClientError
from extract import (s3_connection, average_sentiment_analysis,
                     extract_s3_data, initialize_trend_request, fetch_suggestions, main)


@pytest.fixture
def aws_env_vars():
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
def test_s3_connection_connection_error(mock_client, aws_env_vars, caplog):
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
    mock_prefix = f'bluesky/{mock_datetime.now}'
    bucket_name = 'bucket_name'
    topics = ['python']

    # Mock response for s3.list_objects_v2
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
    mock_prefix = f'bluesky/{mock_datetime.now}'
    bucket_name = 'bucket_name'
    topics = ['python']

    mock_client.list_objects_v2.return_value = {}
    with caplog.at_level(logging.INFO):
        with pytest.raises(ValueError):
            extract_s3_data(mock_client, bucket_name, topics)


@patch('extract.initialize_trend_request')
@patch('extract.extract_s3_data')
@patch('extract.s3_connection')
def test_main_success(mock_s3_conn, mock_extract_s3, mock_pytrend, aws_env_vars):
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

    expected_data = [
        {'Hour': '00', 'Keyword': "python", "Average Sentiment": 0.6, "Total Mentions": 2,
         "Related Terms": "python tutorial,python programming"},
        {"Hour": "01", "Keyword": "python", "Average Sentiment": -0.3, "Total Mentions": 2,
         "Related Terms": "python tutorial,python programming"}
    ]

    result = main(topic)
    assert isinstance(result, pd.DataFrame)
    expected_columns = {'Hour', 'Keyword',
                        'Average Sentiment', 'Total Mentions', 'Related Terms'}

    assert set(result.columns) == expected_columns
    mock_extract_s3.assert_called_once_with(mock_s3, 'bucket_name', ['python'])
    mock_pytrend.assert_called_once()


# @patch.dict('os.environ', {'S3_BUCKET_NAME': 'fake_bucket'}, clear=True)
# @patch('extract.s3_connection')
# def test_fetch_file_content_success(mock_s3):
#     """Test successful file content fetched."""
#     mock_file_obj = MagicMock()
#     mock_file_content = 'mock keyword in this topic'
#     mock_file_obj['Body'].read.return_value.decode.return_value = mock_file_content
#     mock_s3.get_object.return_value = mock_file_obj

#     result = fetch_file_content(mock_s3, 'mock_filename', ['topic'])
#     assert result.get('Text') == mock_file_content


# @patch.dict('os.environ', {'S3_BUCKET_NAME': 'fake_bucket'}, clear=True)
# @patch('extract.s3_connection')
# def test_unsuccessful_file_content_not_found(mock_s3):
#     """Test case when a file is not found with keyword."""
#     mock_file_obj = MagicMock()
#     mock_file_content = 'This is a file with no matching keywords.'
#     mock_file_obj['Body'].read.return_value.decode.return_value = mock_file_content
#     mock_s3.get_object.return_value = mock_file_obj
#     result = fetch_file_content(mock_s3, 'mock_filename', ['topic'])
#     assert result is None


# @patch.dict('os.environ', {'S3_BUCKET_NAME': 'fake_bucket'}, clear=True)
# @patch('extract.s3_connection')
# def test_unsuccessful_file_content_filenotfound(mock_s3, caplog):
#     """Test when no files in S3 bucket, error is handled gracefully."""

#     mock_s3.get_object.side_effect = ClientError(
#         {"Error": {"Code": "NoSuchKey", "Message": "The specified key does not exist"}},
#         'GetObject'
#     )

#     with pytest.raises(FileNotFoundError):
#         fetch_file_content(mock_s3, 'mock_filename', ['topic'])
#     assert 'No files found in S3: An error occurred (NoSuchKey) when calling the GetObject operation: The specified key does not exist' in caplog.text

#     for record in caplog.records:
#         assert record.levelname == 'ERROR'


# @patch.dict('os.environ', {'S3_BUCKET_NAME': 'fake_bucket'}, clear=True)
# @patch('extract.s3_connection')
# def test_successful_extract_bluesky_files(mock_s3):
#     """Test successful extraction of bluesky files from S3 bucket."""
#     mock_first_page = {
#         'Contents': [
#             {'Key': 'file1.txt'},
#             {'Key': 'file2.txt'}
#         ],
#         'NextContinuationToken': 'fake_token'
#     }
#     mock_second_page = {
#         'Contents': [
#             {'Key': 'file3.txt'},
#             {'Key': 'file4.txt'}
#         ],
#         'NextContinuationToken': None
#     }
#     mock_s3.list_objects_v2.side_effect = [mock_first_page, mock_second_page]

#     result = extract_bluesky_files(mock_s3)
#     assert result == ['file1.txt', 'file2.txt', 'file3.txt', 'file4.txt']
#     assert mock_s3.list_objects_v2.call_count == 2


# @patch.dict(os.environ, {}, clear=True)
# @patch('extract.s3_connection')
# def test_unsuccessful_extract_bluesky_missing_env(mock_s3):
#     """Test for case when missing .env variables."""
#     with pytest.raises(KeyError):
#         extract_bluesky_files(mock_s3)


# @patch('extract.s3_connection')
# @patch('extract.fetch_file_content')
# def test_multi_threading_matching_successful(mock_fetch, mock_s3):
#     """Test multithreading works as planned to give expected results."""
#     mock_fetch.side_effect = [
#         {'Text': 'File content 1 with topic1', 'Keyword': 'topic1'},
#         None,
#         {'Text': 'File content 3 with topic3', 'Keyword': 'topic3'},
#     ]
#     topics = ['topic1', 'topic2']
#     filenames = ['file1', 'file2', 'file3']
#     result = multi_threading_matching(mock_s3, topics, filenames)
#     assert len(result) == 2
#     assert {'Text': 'File content 1 with topic1',
#             'Keyword': 'topic1'} in result


# @patch.dict('os.environ', {'AWS_ACCESS_KEY_ID': 'fake_access_key', 'AWS_SECRET_ACCESS_KEY': 'fake_secret_key', 'S3_BUCKET_NAME': 'fake_bucket'})
# @patch('extract.multi_threading_matching')
# @patch('extract.s3_connection')
# @patch('extract.fetch_file_content')
# def test_create_dataframe(mock_fetch, mock_s3, mock_multithread):
#     """Test that the overall result from the create dataframe is correct."""
#     topics = ['topic1', 'topic2']

#     mock_s3_client = MagicMock()
#     mock_s3_client.list_objects_v2.return_value = {
#         'Contents': [
#             {'Key': 'file1.txt'},
#             {'Key': 'file2.txt'}
#         ]
#     }
#     mock_s3.return_value = mock_s3_client

#     mock_multithread.return_value = [{
#         'Text': 'File content 1 with topic1', 'Keyword': 'topic1'},
#         {'Text': 'File content 2 with topic2', 'Keyword': 'topic2'}]
#     mock_fetch.return_value = [{
#         'Text': 'File content 1 with topic1', 'Keyword': 'topic1'},
#         {'Text': 'File content 2 with topic2', 'Keyword': 'topic2'}]
#     result = create_dataframe(topics)
#     assert isinstance(result, pd.DataFrame)
#     assert 'Text' in result.columns
#     assert 'Keyword' in result.columns


# @patch.dict('os.environ', {'AWS_ACCESS_KEY_ID': 'fake_access_key', 'AWS_SECRET_ACCESS_KEY': 'fake_secret_key', 'S3_BUCKET_NAME': 'fake_bucket'})
# @patch('extract.multi_threading_matching')
# @patch('extract.s3_connection')
# @patch('extract.fetch_file_content')
# def test_unsuccessful_create_dataframe(mock_fetch, mock_s3, mock_multithread):
#     """Test that the overall result from the create dataframe is correct."""
#     topics = ['topic1', 'topic2']

#     mock_s3_client = MagicMock()
#     mock_s3_client.list_objects_v2.return_value = {
#         'Contents': [
#             {'Key': 'file1.txt'},
#             {'Key': 'file2.txt'}
#         ]
#     }
#     mock_s3.return_value = mock_s3_client
#     mock_multithread.return_value = []
#     with pytest.raises(ValueError):
#         create_dataframe(topics)


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


# @patch.dict('os.environ', {'AWS_ACCESS_KEY_ID': 'fake_access_key', 'AWS_SECRET_ACCESS_KEY': 'fake_secret_key', 'S3_BUCKET_NAME': 'fake_bucket'})
# @patch('extract.TrendReq')
# @patch('extract.multi_threading_matching')
# @patch('extract.s3_connection')
# @patch('extract.fetch_file_content')
# def test_main_success(mock_fetch, mock_s3, mock_multithread, mock_trendreq):
#     """Test main function will extract data from s3 and google trends."""
#     topics = ['topic1']

#     mock_s3_client = MagicMock()
#     mock_s3_client.list_objects_v2.return_value = {
#         'Contents': [
#             {'Key': 'file1.txt'},
#             {'Key': 'file2.txt'}
#         ]
#     }
#     mock_s3.return_value = mock_s3_client

#     mock_multithread.return_value = [{
#         'Text': 'File content 1 with topic1', 'Keyword': 'topic1'},
#         {'Text': 'File content 2 with topic1', 'Keyword': 'topic1'}]
#     mock_fetch.return_value = [{
#         'Text': 'File content 1 with topic1', 'Keyword': 'topic1'},
#         {'Text': 'File content 2 with topic1', 'Keyword': 'topic1'}]
#     mock_pytrend_instance = MagicMock()

#     mock_related_words = [
#         {'mid': 'i', 'title': 'topic2', 'type': 'random'}]
#     mock_pytrend_instance.suggestions.return_value = mock_related_words
#     mock_trendreq.return_value = mock_pytrend_instance

#     result = main(topics)
#     assert isinstance(result, pd.DataFrame)
#     assert 'Related Terms' in result.columns
#     topic1_related = result[result['Keyword']
#                             == 'topic1']['Related Terms'].iloc[0]
#     expected_related1 = 'topic2'
#     assert topic1_related == expected_related1
