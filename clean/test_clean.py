"""Test file for clean.py"""

import pytest
import os
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, patch
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
from clean import (s3_connection, lambda_handler)


@patch.dict('os.environ', {'AWS_ACCESS_KEY_ID': 'fake_access_key', 'AWS_SECRET_ACCESS_KEY': 'fake_secret_key'})
@patch('clean.client')
def test_successful_s3_connection(mock_client):
    """Test the successful connection to an S3 client without real-world side effects."""

    s3_connection()
    mock_client.assert_called_once_with(
        's3',
        'fake_access_key',
        'fake_secret_key'
    )


@patch.dict(os.environ, {}, clear=True)
@patch('clean.client')
def test_unsuccessful_s3_connection_missing_env(mock_client, caplog):
    """Test that missing env variables will be handled gracefully. """
    with pytest.raises(Exception):
        s3_connection()
    mock_client.assert_not_called()
    assert 'Missing required AWS credentials in .env file' in caplog.text


@patch.dict('os.environ', {'AWS_ACCESS_KEY_ID': 'fake_access_key', 'AWS_SECRET_ACCESS_KEY': 'fake_secret_key'})
@patch('clean.client')
def test_s3_connection_raises_nocredentialserror(mock_client, caplog):
    """Test that s3 connection function can and will raise the NoCredentialsError."""
    mock_client.side_effect = NoCredentialsError()
    with pytest.raises(NoCredentialsError):
        s3_connection()
    assert 'A BotoCore error occurred: Unable to locate credentials' in caplog.text

    mock_client.assert_called_once_with(
        's3', 'fake_access_key', 'fake_secret_key')


@patch.dict('os.environ', {'AWS_ACCESS_KEY_ID': 'fake_access_key', 'AWS_SECRET_ACCESS_KEY': 'fake_secret_key'})
@patch('clean.client')
def test_s3_connection_raises_partialcredentialserror(mock_client, caplog):
    """Test that s3 connection function can and will raise the PartialCredentialsError."""
    mock_client.side_effect = PartialCredentialsError(
        provider='aws', cred_var='AWS_SECRET_ACCESS_KEY')
    with pytest.raises(PartialCredentialsError):
        s3_connection()
    assert 'A BotoCore error occurred: Partial credentials found in aws' in caplog.text

    mock_client.assert_called_once_with(
        's3', 'fake_access_key', 'fake_secret_key')


@patch('clean.s3_connection')
def test_no_contents_response_lambda(mock_s3, caplog):
    """Test message is returned when nothing in the bucket to clean."""
    mock_s3_instance = MagicMock()
    mock_s3.return_value = mock_s3_instance

    mock_s3_instance.list_objects_v2.return_value = {'not': 'contents'}
    mock_event = MagicMock()
    mock_context = MagicMock()

    with caplog.at_level('INFO'):
        result = lambda_handler(mock_event, mock_context)
    assert 'No objects found in the bucket.'
    assert result.get('status') == 'No objects to clean'


@patch('clean.datetime')
@patch('clean.s3_connection')
def test_objects_within_retention_lambda(mock_s3, mock_datetime, caplog):
    """Test that objects in bucket contents are within retention period."""

    mock_datetime.now.return_value = datetime(
        2024, 12, 5, 0, 0, 0, tzinfo=timezone.utc)
    mock_s3_instance = MagicMock()
    mock_s3.return_value = mock_s3_instance

    current_time = datetime(2024, 12, 5, 16, 0, 0, tzinfo=timezone.utc)
    recent_time1 = current_time - timedelta(days=3)
    recent_time2 = current_time - timedelta(days=2)
    mock_s3_instance.list_objects_v2.return_value = {
        "Contents": [
            {"Key": "recent-file1.txt", "LastModified": recent_time1},
            {"Key": "recent-file2.txt", "LastModified": recent_time2},
        ]
    }

    mock_event = MagicMock()
    mock_context = MagicMock()
    with caplog.at_level('INFO'):
        result = lambda_handler(mock_event, mock_context)
    mock_s3_instance.list_objects_v2.assert_called_once()
    assert 'Object is within retention period:' in caplog.text
    assert result == {"status": "Completed", "deleted_files": []}


@patch('clean.datetime')
@patch('clean.s3_connection')
def test_objects_deleted_lambda(mock_s3, mock_datetime, caplog):
    """Test that objects in bucket contents are not within retention period, so get deleted."""

    mock_datetime.now.return_value = datetime(
        2024, 12, 5, 0, 0, 0, tzinfo=timezone.utc)
    mock_s3_instance = MagicMock()
    mock_s3.return_value = mock_s3_instance

    current_time = datetime(2024, 12, 5, 16, 0, 0, tzinfo=timezone.utc)
    old_time1 = current_time - timedelta(days=8)
    old_time2 = current_time - timedelta(days=9)
    mock_s3_instance.list_objects_v2.return_value = {
        "Contents": [
            {"Key": "recent-file1.txt", "LastModified": old_time1},
            {"Key": "recent-file2.txt", "LastModified": old_time2},
        ]
    }

    mock_event = MagicMock()
    mock_context = MagicMock()
    with caplog.at_level('INFO'):
        result = lambda_handler(mock_event, mock_context)
    mock_s3_instance.list_objects_v2.assert_called_once()
    assert 'Deleted old object:' in caplog.text
    assert result == {"status": "Completed",
                      "deleted_files": ['recent-file1.txt', 'recent-file2.txt']}
    assert mock_s3_instance.delete_object.call_count == 2


@patch('clean.datetime')
@patch('clean.s3_connection')
def test_lambda_exception(mock_s3, mock_datetime, caplog):
    """Test an exception is raised if something goes wrong when deleting the object."""

    mock_datetime.now.return_value = datetime(
        2024, 12, 5, 0, 0, 0, tzinfo=timezone.utc)
    mock_s3_instance = MagicMock()
    mock_s3.return_value = mock_s3_instance

    current_time = datetime(2024, 12, 5, 16, 0, 0, tzinfo=timezone.utc)
    old_time1 = current_time - timedelta(days=8)
    old_time2 = current_time - timedelta(days=9)
    mock_s3_instance.list_objects_v2.return_value = {
        "Contents": [
            {"Key": "recent-file1.txt", "LastModified": old_time1},
            {"Key": "recent-file2.txt", "LastModified": old_time2},
        ]
    }

    mock_s3_instance.delete_object.side_effect = Exception()

    mock_event = MagicMock()
    mock_context = MagicMock()
    with caplog.at_level('INFO'):
        result = lambda_handler(mock_event, mock_context)
    mock_s3_instance.list_objects_v2.assert_called_once()
    assert 'Error during cleanup' in caplog.text
    assert result == {'error': '', 'status': 'Failed'}
    for record in caplog.records:
        assert record.levelname == 'ERROR'


@patch('clean.datetime')
@patch('clean.s3_connection')
def test_objects_exactly_at_retention_7(mock_s3, mock_datetime, caplog):
    """Test that object in bucket contents is exactly within retention period, so will not get deleted."""

    mock_datetime.now.return_value = datetime(
        2024, 12, 5, 0, 0, 0, tzinfo=timezone.utc)
    mock_s3_instance = MagicMock()
    mock_s3.return_value = mock_s3_instance

    current_time = datetime(2024, 12, 5, 16, 0, 0, tzinfo=timezone.utc)
    old_time1 = current_time - timedelta(days=7)

    mock_s3_instance.list_objects_v2.return_value = {
        "Contents": [
            {"Key": "recent-file1.txt", "LastModified": old_time1}
        ]
    }

    mock_event = MagicMock()
    mock_context = MagicMock()
    with caplog.at_level('INFO'):
        result = lambda_handler(mock_event, mock_context)
    mock_s3_instance.list_objects_v2.assert_called_once()
    assert 'Object is within retention period:' in caplog.text
    assert result == {"status": "Completed", "deleted_files": []}
