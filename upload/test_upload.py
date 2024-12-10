"""Test script for upload.py"""
# pylint: skip-file

import logging
import pytest
import os
from freezegun import freeze_time
from unittest.mock import MagicMock, patch, ANY
from atproto import models
from botocore.exceptions import ClientError, EndpointConnectionError
from upload import (s3_connection, format_text, extract_text_from_bytes,
                    get_firehose_data, start_firehose_extraction, connect_and_upload,
                    upload_to_s3)


@pytest.fixture
def sample_bytes():
    return b'''{"text": "Hello World", "$type": "app.bsky.feed.post", "langs": ["en"], "createdAt": "2024-12-03T11:17:35.355Z"}'''


@pytest.fixture
def topics():
    return ["cloud", "sky"]


@patch.dict('os.environ', {'AWS_ACCESS_KEY_ID': 'fake_access_key', 'AWS_SECRET_ACCESS_KEY': 'fake_secret_key'})
@patch('upload.client')
def test_successful_s3_connection(mock_client):
    """Test the successful connection to an S3 client without real-world side effects."""

    s3_connection()
    mock_client.assert_called_once_with(
        's3',
        'fake_access_key',
        'fake_secret_key'
    )


@patch.dict(os.environ, {}, clear=True)
@patch('upload.client')
def test_unsuccessful_s3_connection_missing_env(mock_client, caplog):
    """Test that missing env variables will be handled gracefully. """
    with pytest.raises(Exception):
        s3_connection()
    mock_client.assert_not_called()
    assert 'Missing required AWS credentials in .env file' in caplog.text


@patch.dict('os.environ', {'AWS_ACCESS_KEY_ID': 'fake_access_key', 'AWS_SECRET_ACCESS_KEY': 'fake_secret_key'})
@patch('upload.client')
def test_s3_connection_raises_client_serror(mock_client, caplog):
    """Test that s3 connection function, if encountering issues like authentication failure, invalid credentials, or incorrect parameters will raise an error."""
    mock_client.side_effect = ClientError(
        error_response={'Error': {'Code': 'AuthFailure', 'Message': 'Authentication failure'}},
        operation_name='connect'
    )
    with pytest.raises(ClientError):
        s3_connection()
    assert 'An AWS ClientError occurred:' in caplog.text

    mock_client.assert_called_once_with(
        's3', 'fake_access_key', 'fake_secret_key')


@patch.dict('os.environ', {'AWS_ACCESS_KEY_ID': 'fake_access_key', 'AWS_SECRET_ACCESS_KEY': 'fake_secret_key'})
@patch('upload.client')
def test_s3_connection_raises_value_error(mock_client, caplog):
    """Test that s3 connection function can and will raise the Value Error if the credentials are missing or incomplete."""
    mock_client.side_effect = ValueError()
    with pytest.raises(ValueError):
        s3_connection()
    assert 'Configuration error:' in caplog.text

    mock_client.assert_called_once_with(
        's3', 'fake_access_key', 'fake_secret_key')

@patch.dict('os.environ', {'AWS_ACCESS_KEY_ID': 'fake_access_key', 'AWS_SECRET_ACCESS_KEY': 'fake_secret_key'})
@patch('upload.client')
def test_s3_connection_raises_exception(mock_client, caplog):
    """Test that s3 connection function will raise an exception against unforeseen errors."""
    mock_client.side_effect = Exception()
    with pytest.raises(Exception):
        s3_connection()
    assert 'An unexpected error occurred while connecting to S3: ' in caplog.text

    mock_client.assert_called_once_with(
        's3', 'fake_access_key', 'fake_secret_key')



def test_correct_formatting():
    """Test the function removes all extra new lines and whitespaces successfully."""
    test_string = "H ello    \n \n \n Good bye!"
    assert format_text(test_string) == "H ello Good bye!"


@patch('upload.json')
def test_successful_extract_text(mock_json, sample_bytes):
    """Parsed = dict, json_data=str, text=str"""
    """Test that bytes are successfully parsed into dict then strings."""

    raw_byte_input = sample_bytes
    mock_json.dumps.return_value = '{"text": "Hello World", "$type": "app.bsky.feed.post", "langs": ["en"], "createdAt": "2024-12-03T11:17:35.355Z"}'
    mock_json.loads.return_value = {"text": "Hello World", "$type": "app.bsky.feed.post", "langs": [
        "en"], "createdAt": "2024-12-03T11: 17: 35.355Z"}
    result = extract_text_from_bytes(raw_byte_input)
    assert result == "Hello World"
    assert isinstance(result, str)


@patch('upload.json')
def test_unsuccessful_extract_text_typeerror(mock_json, caplog):
    """Test a type error will be raised with str input instead of bytes."""

    string_input = '{"text": "Hello World", "$type": "app.bsky.feed.post", "langs": ["en"], "createdAt": "2024-12-03T11:17:35.355Z"}'

    mock_json.dumps.side_effect = TypeError(
        "Simulated json dumps error")
    with caplog.at_level(logging.WARNING):
        print(extract_text_from_bytes(string_input))
    assert "Error extracting text: Simulated json dumps error" in caplog.text
    mock_json.loads.assert_not_called()


@patch('upload.json')
@patch('upload.format_text')
def test_unsuccessful_extract_text_attributeerror(mock_format_text, mock_json, caplog):
    """Test function to ensure an attribute error is logged if str cannot be parsed."""

    raw_byte_input = b'["text": "Hello World", "$type": "app.bsky.feed.post", "langs": ["en"], "createdAt": "2024-12-03T11:17:35.355Z"]'
    mock_json.dumps.return_value = '["text": "Hello World", "$type": "app.bsky.feed.post", "langs": ["en"], "createdAt": "2024-12-03T11:17:35.355Z"]'
    mock_json.loads.side_effect = AttributeError(
        "Simulated json dumps error")
    with caplog.at_level(logging.WARNING):
        extract_text_from_bytes(raw_byte_input)
        assert "Error extracting text: Simulated json dumps error" in caplog.text

    mock_format_text.assert_not_called()


@patch('upload.extract_text_from_bytes')
@patch('upload.parse_subscribe_repos_message')
@patch('upload.logging')
@patch('upload.upload_to_s3')
def test_firehose_invalid_commit_type(mock_s3_upload, mock_logging, mock_parse_subscribe_repos_message, mock_extract, sample_bytes, topics):
    """test that if repo_commit is not the correct type, the function will exit early."""

    mock_parse_subscribe_repos_message.return_value = "Invalid Commit Object"
    get_firehose_data(sample_bytes)

    mock_s3_upload.assert_not_called()
    mock_logging.info.assert_not_called()
    mock_extract.assert_not_called()
    mock_logging.error.assert_not_called()


@patch('upload.get_or_create')
@patch('upload.CAR')
@patch('upload.parse_subscribe_repos_message')
@patch('upload.upload_to_s3')
def test_invalid_processed_post_type(mock_s3_upload, mock_parse_subscribe_repos_message, mock_CAR, mock_get_create):
    """Test that posts with an invalid type (i.e. not 'app.bsky.feed.post') are not processed to S3 upload."""
    message = b"test message"

    # Create a mock commit object with correct type, action and CID which is returned from parse func
    mock_repo_commit = MagicMock()
    mock_repo_commit.__class__ = models.ComAtprotoSyncSubscribeRepos.Commit
    mock_repo_commit.ops = [MagicMock(action="create", cid="valid_cid")]
    mock_parse_subscribe_repos_message.return_value = mock_repo_commit

    # Create mock CAR file instance which returns raw bytes
    mock_car_instance = MagicMock()
    mock_car_instance.blocks = MagicMock()
    mock_car_instance.blocks.get = MagicMock(return_value=b"raw bytes")
    mock_CAR.return_value = mock_car_instance

    # Set mock processed post to have incorrect py_type i.e. not a BlueSky feed post
    mock_processed_post = MagicMock(py_type="test.type")
    mock_get_create.return_value = mock_processed_post

    get_firehose_data(message)

    mock_s3_upload.assert_not_called()


@patch('upload.upload_to_s3')
@patch('upload.extract_text_from_bytes')
@patch('upload.get_or_create')
@patch('upload.CAR.from_bytes')
@patch('upload.parse_subscribe_repos_message')
def test_get_firehose_data_keyword_match(mock_parse, mock_CAR, mock_get_or_create, mock_extract, mock_s3_upload, caplog):
    """Test writing a row when a keyword is found in a post."""

    mock_repo_commit = MagicMock(
        spec=models.ComAtprotoSyncSubscribeRepos.Commit)
    mock_repo_commit.ops = [MagicMock(action='create', cid='mock_cid')]
    mock_repo_commit.blocks = MagicMock()
    mock_parse.return_value = mock_repo_commit

    mock_car_instance = MagicMock()
    mock_car_instance.blocks = MagicMock()
    mock_car_instance.blocks.get = MagicMock(return_value=b'raw bytes')
    mock_CAR.return_value = mock_car_instance

    mock_processed_post = MagicMock(py_type='app.bsky.feed.post')
    mock_get_or_create.return_value = mock_processed_post

    mock_extract.return_value = 'cloud in the sky'
    mock_s3_upload.return_value = None
    with caplog.at_level(logging.INFO):
        message = b"clouds in the sky"
        get_firehose_data(message)
    assert 'Extracted text: ' in caplog.text
    mock_s3_upload.assert_called_with('cloud in the sky')


@patch("ssl.create_default_context")
@patch("certifi.where")
@patch("builtins.open")
@patch("os.makedirs")
@patch("upload.FirehoseSubscribeReposClient")
def test_ssl_context_and_client_initialisation(mock_client, mock_make_dirs, mock_open, mock_certifi_where, mock_create_default_context, topics):
    """Test the creation of a mock SSL context (used to establish secure connection by firehose client) 
    and the correct handling of the certificate path and Firehose client instantiation, without real-world
    side effects."""

    mock_ssl_context = MagicMock()
    # Ensuring function call receives mock ssl context and mock certificate filepath
    mock_create_default_context.return_value = mock_ssl_context
    mock_certifi_where.return_value = "/mock/path/to/certificate.pem"

    # Mock firehose client without connecting on network with mock ssl context
    mock_client_instance = MagicMock()
    mock_client.return_value = mock_client_instance
    mock_client_instance.ssl_context = mock_ssl_context

    connect_and_upload()

    mock_create_default_context.assert_called_once_with(
        cafile="/mock/path/to/certificate.pem")
    mock_client.assert_called_once()


@patch('upload.get_firehose_data')
@patch('upload.FirehoseSubscribeReposClient')
def test_start_firehose_extraction_success(mock_firehose_client, mock_get_data):
    """Test that messages can be retrieved from the Firehose Client."""
    mock_firehose_client_instance = MagicMock()
    mock_firehose_client.start().return_value = mock_firehose_client_instance
    start_firehose_extraction(mock_firehose_client)

    mock_firehose_client.start.assert_called()
    lambda_function = mock_firehose_client.start.call_args[0][0]

    lambda_function('message')
    mock_get_data.assert_called_once_with('message')
    assert callable(lambda_function)


@freeze_time("2000-12-03 16:11:16")
@patch.dict('os.environ', {'AWS_ACCESS_KEY_ID': 'fake_access_key', 'AWS_SECRET_ACCESS_KEY': 'fake_secret_key', 'S3_BUCKET_NAME': 'bucket'})
@patch('upload.s3_connection')
def test_successful_upload_to_s3(mock_s3, caplog):
    """Test content is uploaded to suitable bucket & object prefix."""
    mock_s3_instance = MagicMock()
    mock_s3.return_value = mock_s3_instance

    mock_s3_key = 'bluesky/2000-12-03/16/20001203161116000000.txt'
    mock_bucket = 'bucket'
    mock_body = 'hello'
    mock_s3_instance.put_object.return_value = None
    with caplog.at_level(logging.INFO):
        result = upload_to_s3(mock_body)

    assert 'Uploaded to S3: ' in caplog.text
    assert result is None
    mock_s3_instance.put_object.assert_called_with(
        Bucket=mock_bucket, Key=mock_s3_key, Body=mock_body)


@patch.dict('os.environ', {'AWS_ACCESS_KEY_ID': 'fake_access_key', 'AWS_SECRET_ACCESS_KEY': 'fake_secret_key'})
@patch('upload.s3_connection')
def test_unsuccessful_upload_s3_endpoint_connection_error(mock_s3_connection, caplog):
    """Test that upload to s3 will raise an error if there are network configuration issues."""
    mock_s3_instance = MagicMock()
    mock_s3_connection.return_value = mock_s3_instance

    mock_s3_connection.side_effect = EndpointConnectionError(endpoint_url='fake_url')
    mock_body = 'hello'
    with pytest.raises(EndpointConnectionError):
        upload_to_s3(mock_body)
    assert 'Failed to connect to the S3 endpoint:' in caplog.text

    mock_s3_connection.assert_called_once()
    mock_s3_instance.put_object.assert_not_called()


@patch.dict('os.environ', {'AWS_ACCESS_KEY_ID': 'fake_access_key', 'AWS_SECRET_ACCESS_KEY': 'fake_secret_key'})
@patch('upload.s3_connection')
def test_unsuccessful_upload_s3_exception(mock_s3_connection, caplog):
    """Test that upload to s3 will raise errors against unforeseen errors."""
    mock_s3_instance = MagicMock()
    mock_s3_connection.return_value = mock_s3_instance

    mock_s3_connection.side_effect = Exception()
    mock_body = 'hello'
    with pytest.raises(Exception):
        upload_to_s3(mock_body)
    assert 'An unexpected error occurred while uploading to S3:' in caplog.text

    mock_s3_connection.assert_called_once()
    mock_s3_instance.put_object.assert_not_called()

@patch.dict('os.environ', {'AWS_ACCESS_KEY_ID': 'fake_access_key', 'AWS_SECRET_ACCESS_KEY': 'fake_secret_key'})
@patch('upload.s3_connection')
def test_unsuccessful_upload_s3_client_error(mock_s3_connection, caplog):
    """Test that upload to s3 will raise an error when catching AWS authentication failures and related permission issues."""

    mock_s3_instance = MagicMock()
    mock_s3_connection.return_value = mock_s3_instance

    mock_s3_connection.side_effect = ClientError(
        error_response={'Error': {'Code': 'AuthFailure', 'Message': 'Authentication failure'}},
        operation_name='connect')
    mock_body = 'hello'
    with pytest.raises(ClientError):
        upload_to_s3(mock_body)
    assert 'An AWS ClientError occurred:' in caplog.text

    mock_s3_connection.assert_called_once()
    mock_s3_instance.put_object.assert_not_called()
