"""Test script for upload.py"""
# pylint: skip-file

import logging
import pytest
from unittest.mock import MagicMock, patch
from atproto import models
from upload import (s3_connection, format_text, extract_text_from_bytes,
                    get_firehose_data, start_firehose_extraction, connect_and_upload,
                    upload_to_s3)


@pytest.fixture
def sample_bytes():
    return b'''{"text": "Hello World", "$type": "app.bsky.feed.post", "langs": ["en"], "createdAt": "2024-12-03T11:17:35.355Z"}'''


@pytest.fixture
def topics():
    return ["cloud", "sky"]


def test_correct_formatting():
    """Test the function removes all extra new lines and whitespaces successfully."""
    test_string = "H ello    \n \n \n Good bye!"
    assert format_text(test_string) == "H ello Good bye!"


@patch('upload.json')
@patch('upload.logging')
def test_successful_extract_text(mock_logging, mock_json, sample_bytes):
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
    """Parsed = dict, json_data=str, text=str"""
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
