"""Test file for etl.py: Moving data from RDS to S3."""
# pylint: skip-file

import pytest
import os
import logging
import pandas as pd
from psycopg2 import OperationalError, InterfaceError, DatabaseError
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError
from unittest.mock import MagicMock, patch
from psycopg2.extras import RealDictCursor
from sqlalchemy.exc import SQLAlchemyError

from etl_lambda import (setup_engine, setup_connection, s3_connection, download_csv_from_s3,
                        upload_to_s3, delete_local_file, fetch_subscription_data_from_rds, clear_keyword_recordings, lambda_handler)


@pytest.fixture()
def configs():
    """Configs for .env file"""
    return {
        "DB_USERNAME": "user",
        "DB_PASSWORD": "password",
        "DB_HOST": "localhost",
        "DB_PORT": "1234",
        "DB_NAME": "name",
        "ACCESS_KEY_ID": "fake_access_key",
        "SECRET_ACCESS_KEY": "fake_secret_key",
        "SCHEMA_NAME": "fake_schema",

    }


@pytest.fixture(autouse=True)
def mock_env(configs):
    """Fixture to mock environment variables."""
    with patch.dict(os.environ, configs, clear=True):
        yield


@patch('etl_lambda.create_engine')
def test_setup_engine(mock_engine, mock_env):
    """Test successful setup of sqlalchemy engine."""
    engine = setup_engine()

    expected_conn_str = f"postgresql+psycopg2://{os.environ['DB_USERNAME']
                                                 }:{os.environ['DB_PASSWORD']}@{os.environ['DB_HOST']}:{
        os.environ['DB_PORT']}/{os.environ['DB_NAME']}"

    mock_engine.assert_called_once_with(expected_conn_str)
    assert engine == mock_engine.return_value


# need to add error hndling for setting up engine
@patch('etl_lambda.create_engine')
def test_SQLAlchemyError_setup_engine(mock_engine, caplog):
    """Test SQLAlchemyError raised when something goes wrong with the SQLAlchemy library."""
    mock_engine.side_effect = SQLAlchemyError('Simulated SQLAlchemyError')

    with caplog.at_level(logging.INFO):
        with pytest.raises(SQLAlchemyError):
            setup_engine()

    assert 'Failed to connect to database:' in caplog.text


@patch('etl_lambda.create_engine')
def test_exception_setup_engine(mock_engine, caplog):
    """Test general exception catches any other wrong-goings."""
    mock_engine.side_effect = Exception('Simulated Exception')

    with caplog.at_level(logging.INFO):
        with pytest.raises(Exception):
            setup_engine()

    assert 'An unexpected error occurred:' in caplog.text


@patch('etl_lambda.psycopg2.connect')
def test_setup_connection_success(mock_connect, mock_env, caplog):
    """Test successful connection and schema setting."""

    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn
    mock_cursor = MagicMock(spec=RealDictCursor)
    mock_conn.cursor.return_value = mock_cursor

    with caplog.at_level(logging.INFO):
        result = setup_connection()
    assert result == (mock_conn, mock_cursor)
    assert 'Connection successfully established to RDS database.' in caplog.text
    mock_conn.cursor.assert_called_once()


@patch('etl_lambda.psycopg2.connect')
def test_setup_connection_operational_error(mock_connect, mock_env, caplog):
    """Test Operational Error is raised when there is a problem connecting to PostgreSQL 
     database or executing database operation."""

    mock_connect.side_effect = OperationalError('Simulated OperationalError')
    with caplog.at_level(logging.INFO):
        with pytest.raises(OperationalError):
            setup_connection()
    assert 'Operational error while connecting to the database' in caplog.text
    mock_connect.cursor.assert_not_called()


@patch('etl_lambda.psycopg2.connect')
def test_setup_connection_interface_error(mock_connect, mock_env, caplog):
    """Test Interface Error is raised when there is a problem with the cursor."""
    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn
    mock_conn.cursor.side_effect = InterfaceError(
        'Simulated InterfaceError')

    with caplog.at_level(logging.INFO):
        with pytest.raises(InterfaceError):
            setup_connection()
    assert 'Interface error while connecting to the database:' in caplog.text
    mock_connect.assert_called_once()


@patch('etl_lambda.psycopg2.connect')
def test_setup_connection_database_error(mock_connect, mock_env, caplog):
    """Test Database Error is raised when there is a problem connecting to database due to server
    configuration for example, i.e. during making the connection."""
    mock_connect.side_effect = DatabaseError('Simulated DatabaseError')

    with caplog.at_level(logging.INFO):
        with pytest.raises(DatabaseError):
            setup_connection()

    assert 'Database error occurred' in caplog.text


@patch('boto3.client')
def test_successful_s3_connection(mock_client):
    """Test the successful connection to an S3 client without real-world side effects."""
    s3_connection()
    mock_client.assert_called_once_with(
        's3', 'fake_access_key', 'fake_secret_key')


@patch.dict(os.environ, {}, clear=True)
@patch('boto3.client')
def test_unsuccessful_s3_connection_missing_env(mock_client, caplog):
    """Test that missing env variables will be handled gracefully. """
    with pytest.raises(Exception):
        s3_connection()
    mock_client.assert_not_called()
    assert 'Missing required AWS credentials in .env file' in caplog.text


@patch('boto3.client')
def test_s3_connection_raises_nocredentialserror(mock_client, caplog):
    """Test that s3 connection function can and will raise the NoCredentialsError."""
    mock_client.side_effect = NoCredentialsError()
    with pytest.raises(NoCredentialsError):
        s3_connection()
    assert 'A BotoCore error occurred: Unable to locate credentials' in caplog.text

    mock_client.assert_called_once_with(
        's3', 'fake_access_key', 'fake_secret_key')


@patch('boto3.client')
def test_s3_connection_raises_partialcredentialserror(mock_client, mock_env, caplog):
    """Test that s3 connection function can and will raise the PartialCredentialsError."""
    mock_client.side_effect = PartialCredentialsError(
        provider='aws', cred_var='SECRET_ACCESS_KEY')
    with pytest.raises(PartialCredentialsError):
        s3_connection()
    assert 'A BotoCore error occurred: Partial credentials found in aws' in caplog.text

    mock_client.assert_called_once_with(
        's3', 'fake_access_key', 'fake_secret_key')


@patch('etl_lambda.s3_connection')
def test_successful_download_csv_s3(mock_s3_conn):
    """Test the successful download of files from the S3 bucket."""
