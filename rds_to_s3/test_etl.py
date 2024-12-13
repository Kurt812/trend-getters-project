"""Test file for etl_lambda.py: Moving data from RDS to S3."""

# pylint: skip-file

import os
import logging
from unittest.mock import MagicMock, patch
import pytest
import pandas as pd
from psycopg2 import (OperationalError, InterfaceError, DatabaseError)
from botocore.exceptions import ClientError, EndpointConnectionError
from psycopg2.extras import RealDictCursor
from sqlalchemy.exc import SQLAlchemyError

from etl_lambda import (setup_engine, setup_connection, s3_connection, download_csv_from_s3, upload_to_s3, delete_local_file,
                        fetch_subscription_data_from_rds, clear_keyword_recordings, lambda_handler, REMOVE_QUERY, UPDATE_QUERY)


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
        "S3_BUCKET_NAME": "bucket_name"

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

    expected_conn_str = f"""postgresql+psycopg2://{os.environ['DB_USERNAME']}:{
        os.environ['DB_PASSWORD']}@{os.environ['DB_HOST']}:{os.environ['DB_PORT']}/{os.environ['DB_NAME']}"""

    mock_engine.assert_called_once_with(expected_conn_str)
    assert engine == mock_engine.return_value


@patch('etl_lambda.create_engine')
def test_sql_alchemy_error_setup_engine(mock_engine, caplog):
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
def test_setup_connection_success(mock_connect, caplog):
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
def test_setup_connection_operational_error(mock_connect, caplog):
    """Test Operational Error is raised when there is a problem connecting to PostgreSQL
     database or executing database operation."""

    mock_connect.side_effect = OperationalError('Simulated OperationalError')
    with caplog.at_level(logging.INFO):
        with pytest.raises(OperationalError):
            setup_connection()
    assert 'Operational error while connecting to the database' in caplog.text
    mock_connect.cursor.assert_not_called()


@patch('etl_lambda.psycopg2.connect')
def test_setup_connection_interface_error(mock_connect, caplog):
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
def test_setup_connection_database_error(mock_connect, caplog):
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
        's3')


@patch.dict(os.environ, {}, clear=True)
@patch('boto3.client')
def test_unsuccessful_s3_connection_missing_env(mock_client, caplog):
    """Test that missing env variables will be handled gracefully. """
    with pytest.raises(Exception):
        s3_connection()
    mock_client.assert_not_called()
    assert 'Missing required AWS credentials in .env file' in caplog.text


@patch('boto3.client')
def test_s3_connection_raises_client_error(mock_client, caplog):
    """Test that s3 connection function can and will raise the NoCredentialsError."""
    mock_client.side_effect = ClientError(error_response={'Error': {'Code': 'AuthFailure', 'Message': 'Authentication failure'}},
                                          operation_name='connect')
    with pytest.raises(ClientError):
        s3_connection()
    assert 'An AWS ClientError occurred:' in caplog.text

    mock_client.assert_called_once_with('s3')


@patch('boto3.client')
def test_s3_connection_raises_endpoint_connection_error(mock_client, caplog):
    """Test that s3 connection function can and will raise the PartialCredentialsError."""
    mock_client.side_effect = EndpointConnectionError(endpoint_url='fake_url')
    with pytest.raises(EndpointConnectionError):
        s3_connection()
    assert 'Failed to connect to the S3 endpoint:' in caplog.text

    mock_client.assert_called_once_with('s3')


@patch('etl_lambda.pd.read_csv')
@patch('etl_lambda.s3_connection')
def test_successful_download_csv_s3(mock_s3_conn, mock_read_csv, caplog):
    """Test the successful download of files from the S3 bucket."""
    mock_s3 = MagicMock()
    mock_s3_conn.return_value = mock_s3
    mock_read_csv.return_value = pd.DataFrame({
        'keywords': ['python'],
        'total mentions': [23],
        'avg sentiment': [0.12]
    })
    file_name = 'test_file.csv'
    with caplog.at_level(logging.INFO):
        df = download_csv_from_s3('test_bucket', 'test/path/', file_name)

    mock_read_csv.assert_called_once_with(f'/tmp/{file_name}')
    assert f'Downloaded /tmp/{file_name} from S3.' in caplog.text
    mock_s3.download_file.assert_called_once()
    assert isinstance(df, pd.DataFrame)


@patch('etl_lambda.pd.read_csv')
@patch('etl_lambda.s3_connection')
def test_download_csv_filenotfound(mock_s3_conn, mock_read_csv, caplog):
    """Test if file was not downloaded correctly or unable to be found locally, FileNotFoundError will be raised."""
    mock_s3 = MagicMock()
    mock_s3_conn.return_value = mock_s3
    mock_s3.download_file.return_value = None
    mock_read_csv.side_effect = FileNotFoundError()
    file_name = 'test_file.csv'
    with caplog.at_level(logging.WARNING):
        download_csv_from_s3('test_bucket', 'test/path/', file_name)

    mock_s3.download_file.assert_called_once_with(
        'test_bucket', 'test/path/', '/tmp/test_file.csv')

    assert 'File /tmp/test_file.csv not found locally:' in caplog.text


@patch('etl_lambda.pd.read_csv')
@patch('etl_lambda.s3_connection')
def test_download_csv_clienterror(mock_s3_conn, mock_read_csv, caplog):
    """Test that if the file cannot be found in bucket, the appropriate error will be raised."""
    mock_s3 = MagicMock()
    mock_s3_conn.return_value = mock_s3

    error_response = {
        'Error': {
            'Code': 'NoSuchKey',
            'Message': 'The specified key does not exist.'
        }
    }
    mock_s3.download_file.side_effect = ClientError(
        error_response=error_response,
        operation_name="DownloadFile"
    )
    file_name = 'test_file.csv'

    with caplog.at_level(logging.ERROR):
        with pytest.raises(ClientError):
            download_csv_from_s3('test_bucket', 'test/path/', file_name)

    mock_s3.download_file.assert_called_once_with(
        'test_bucket', 'test/path/', '/tmp/test_file.csv'
    )
    mock_read_csv.assert_not_called()
    assert 'Failed to download /tmp/test_file.csv from S3 (AWS Client Error)' in caplog.text


@patch('etl_lambda.pd.read_csv')
@patch('etl_lambda.s3_connection')
def test_download_csv_emptydataerror(mock_s3_conn, mock_read_csv, caplog):
    """Test that if the file cannot be found in bucket, the appropriate error will be raised."""
    mock_s3 = MagicMock()
    mock_s3_conn.return_value = mock_s3

    bucket_name = 'test_bucket'
    s3_file_path = 'test/path/test_file.csv'
    file_name = 'test_file.csv'

    mock_read_csv.side_effect = pd.errors.EmptyDataError(
        f'Downloaded file /tmp/{file_name} is empty')
    with caplog.at_level(logging.WARNING):
        result = download_csv_from_s3(bucket_name, s3_file_path, file_name)

    mock_s3.download_file.assert_called_once_with(
        bucket_name, s3_file_path, f'/tmp/{file_name}')
    mock_read_csv.assert_called_once_with(f'/tmp/{file_name}')
    assert 'Downloaded file /tmp/test_file.csv is empty' in caplog.text
    assert isinstance(result, pd.DataFrame)
    assert result.empty


@patch('etl_lambda.pd.read_csv')
@patch('etl_lambda.s3_connection')
def test_download_csv_Exception(mock_s3_conn, mock_read_csv, caplog):
    """Test that if any exception occurs, it will be raised."""

    mock_s3 = MagicMock()
    mock_s3_conn.return_value = mock_s3

    mock_s3.download_file.side_effect = Exception()

    bucket_name = "test_bucket"
    s3_file_path = "test/path/test_file.csv"
    file_name = "test_file.csv"

    with caplog.at_level(logging.INFO):
        with pytest.raises(Exception):
            download_csv_from_s3(bucket_name, s3_file_path, file_name)
    assert 'Unexpected error while downloading' in caplog.text
    for record in caplog.records:
        assert record.levelname == 'ERROR'
    mock_read_csv.assert_not_called()


@patch('etl_lambda.s3_connection')
def test_successful_upload_to_s3(mock_s3_conn, caplog):
    """Test successful upload of file to s3 bucket."""

    mock_s3 = MagicMock()
    mock_s3_conn.return_value = mock_s3
    file_name = "test_file.csv"
    object_name = "object_name"
    bucket_name = "test_bucket"

    with caplog.at_level(logging.INFO):
        upload_to_s3(bucket_name, file_name, object_name)

    assert f'Uploaded {
        file_name} to s3://{bucket_name}/{object_name}' in caplog.text
    mock_s3.upload_file.assert_called_once()


@patch('etl_lambda.s3_connection')
def test_upload_s3_filenotfound(mock_s3_conn, caplog):
    """Test if file is unable to be found locally, FileNotFoundError will be raised."""
    mock_s3 = MagicMock()
    mock_s3_conn.return_value = mock_s3

    mock_s3.upload_file.side_effect = FileNotFoundError()
    file_name = 'test_file.csv'
    with caplog.at_level(logging.WARNING):
        with pytest.raises(FileNotFoundError):
            upload_to_s3('test_bucket', file_name,  'object_name')

    mock_s3.upload_file.assert_called_once_with(
        f'/tmp/{file_name}', 'test_bucket', 'object_name')

    assert f'Local file {file_name} not found for upload:' in caplog.text


@patch('etl_lambda.s3_connection')
def test_upload_s3_clienterror(mock_s3_conn, caplog):
    """Test that if the file cannot be found in bucket, the appropriate error will be raised."""
    mock_s3 = MagicMock()
    mock_s3_conn.return_value = mock_s3

    error_response = {
        'Error': {
            'Code': 'NoSuchKey',
            'Message': 'The specified key does not exist.'
        }
    }
    mock_s3.upload_file.side_effect = ClientError(
        error_response=error_response,
        operation_name='UploadFile'
    )

    file_name = 'test_file.csv'
    object_name = 'object_name'
    bucket_name = 'test_bucket'

    with caplog.at_level(logging.ERROR):
        with pytest.raises(ClientError):
            upload_to_s3(bucket_name, file_name, object_name)

    mock_s3.upload_file.assert_called_once_with(
        f'/tmp/{file_name}', bucket_name, object_name)

    assert f'Failed to upload {
        file_name} to S3 (AWS Client Error): ' in caplog.text


@patch('etl_lambda.s3_connection')
def test_upload_s3_exception(mock_s3_conn, caplog):
    """Test if file is unable to be found locally, FileNotFoundError will be raised."""
    mock_s3 = MagicMock()
    mock_s3_conn.return_value = mock_s3

    mock_s3.upload_file.side_effect = Exception()
    file_name = 'test_file.csv'
    with caplog.at_level(logging.WARNING):
        with pytest.raises(Exception):
            upload_to_s3('test_bucket', file_name,  'object_name')

    mock_s3.upload_file.assert_called_once_with(
        f'/tmp/{file_name}', 'test_bucket', 'object_name')

    assert f'Unexpected error while uploading {
        file_name} to S3: ' in caplog.text


@patch('os.path.exists', return_value=True)
@patch('os.remove')
def test_delete_local_file_success(mock_remove, mock_exists, caplog):
    """Test to ensure the specified file is deleted if it exists."""
    file_name = 'keep_file.csv'

    with caplog.at_level(logging.INFO):
        delete_local_file(file_name)

    mock_exists.assert_called_once_with(f'/tmp/{file_name}')
    mock_remove.assert_called_once_with(f'/tmp/{file_name}')

    assert f'Deleted local file: /tmp/{file_name}' in caplog.text


@patch('os.path.exists', return_value=False)
@patch('os.remove')
def test_delete_local_file_warning(mock_remove, mock_exists, caplog):
    """Test to ensure the specified file is deleted if it exists."""
    file_name = 'keep_file.csv'

    with caplog.at_level(logging.WARNING):
        delete_local_file(file_name)

    mock_exists.assert_called_once_with(f'/tmp/{file_name}')
    mock_remove.assert_not_called()

    assert f'File /tmp/{file_name} does not exist.' in caplog.text


@patch('os.path.exists', return_value=True)
@patch('os.remove')
def test_delete_local_file_permissionerror(mock_remove, mock_exists, caplog):
    """Test to ensure the specified file is deleted if it exists."""
    file_name = 'keep_file.csv'
    mock_remove.side_effect = PermissionError()

    with pytest.raises(PermissionError):
        with caplog.at_level(logging.INFO):
            delete_local_file(file_name)

    mock_exists.assert_called_once_with(f'/tmp/{file_name}')
    mock_remove.assert_called_once()

    assert f'Permission denied while trying to delete /tmp/{
        file_name}: ' in caplog.text


@patch('etl_lambda.delete_local_file')
@patch('etl_lambda.upload_to_s3')
@patch('etl_lambda.pd.DataFrame.to_csv')
@patch('etl_lambda.download_csv_from_s3')
@patch('etl_lambda.pd.read_sql')
@patch('etl_lambda.setup_engine')
def test_fetch_sub_data_rds_success(mock_setup_engine, mock_read_sql, mock_download, mock_to_csv, mock_upload, mock_delete_file):
    """Test that the latest data can be fetched from the RDS successfully."""
    mock_engine = MagicMock()
    mock_setup_engine.return_value = mock_engine

    mock_new_data = pd.DataFrame({
        'keywords': ['python'],
        'total mentions': [23],
        'avg sentiment': [0.12]
    })
    mock_read_sql.return_value = mock_new_data

    mock_existing_data = pd.DataFrame({
        'keywords': ['hello'],
        'total mentions': [36],
        'avg sentiment': [0.87]
    })
    mock_download.return_value = mock_existing_data

    fake_query = 'fake query'
    file_name = 'test_file.csv'
    folder_name = 'folder_name'
    bucket_name = 'test_bucket'
    file_path = f'{folder_name}/{file_name}'

    fetch_subscription_data_from_rds(
        fake_query, file_name, bucket_name, folder_name)
    mock_read_sql.assert_called_once_with(fake_query, mock_engine)
    mock_download.assert_called_once_with(bucket_name, file_path, file_name)

    mock_to_csv.assert_called_once_with(f'/tmp/{file_name}', index=False)
    mock_upload.assert_called_once_with(
        bucket_name, file_name, file_path)
    mock_delete_file.assert_called_once_with(file_name)


@patch('etl_lambda.download_csv_from_s3')
@patch('etl_lambda.pd.read_sql')
@patch('etl_lambda.setup_engine')
def test_fetch_sub_data_rds_exception(mock_setup_engine, mock_read_sql, mock_download, caplog):
    """Test that if the fetch query cannot be executed, exception is raised."""
    mock_engine = MagicMock()
    mock_setup_engine.return_value = mock_engine
    fake_query = 'fake query'
    file_name = 'test_file.csv'
    folder_name = 'folder_name'
    bucket_name = 'test_bucket'
    mock_read_sql.side_effect = Exception()
    with caplog.at_level(logging.INFO):
        with pytest.raises(Exception):
            fetch_subscription_data_from_rds(
                fake_query, file_name, bucket_name, folder_name)

    mock_read_sql.assert_called_once_with(fake_query, mock_engine)
    assert 'Error while executing query:' in caplog.text
    mock_download.assert_not_called()


@patch('etl_lambda.setup_connection')
def test_clear_keyword_recordings_success(mock_setup_conn):
    """Test successful clearance of data older than 24 hours from the RDS."""
    mock_conn = MagicMock()
    mock_curs = MagicMock()
    mock_setup_conn.return_value = mock_conn, mock_curs

    remove_query = REMOVE_QUERY
    clear_keyword_recordings()

    mock_curs.execute.assert_called_once_with(remove_query)
    mock_conn.commit.assert_called_once()
    mock_conn.close.assert_called_once()


@patch('etl_lambda.setup_connection')
def test_clear_keyword_recordings_exception(mock_setup_conn, caplog):
    """Test successful clearance of data older than 24 hours from the RDS."""
    mock_conn = MagicMock()
    mock_curs = MagicMock()
    mock_setup_conn.return_value = mock_conn, mock_curs

    remove_query = REMOVE_QUERY
    mock_curs.execute.side_effect = Exception()
    with caplog.at_level(logging.INFO):
        with pytest.raises(Exception):
            clear_keyword_recordings()

    mock_curs.execute.assert_called_once_with(remove_query)
    mock_conn.commit.assert_not_called()
    mock_conn.close.assert_called_once()
    assert 'Error while executing query:' in caplog.text


@patch.dict('etl_lambda.ENV', {"S3_BUCKET_NAME": "test_bucket"}, clear=True)
@patch('etl_lambda.clear_keyword_recordings')
@patch('etl_lambda.fetch_subscription_data_from_rds')
def test_lambda_handler_success(mock_fetch_rds, mock_clear_recs):
    """Test successful lambda handler function."""
    file_name = 'keyword_recording.csv'
    folder_name = 'long_term_keyword_data'
    bucket_name = 'test_bucket'

    lambda_handler(MagicMock(), MagicMock())

    mock_fetch_rds.assert_called_once_with(
        UPDATE_QUERY, file_name, bucket_name, folder_name)
    mock_clear_recs.assert_called_once()


@patch.dict('etl_lambda.ENV', {"S3_BUCKET_NAME": "test_bucket"}, clear=True)
@patch('etl_lambda.clear_keyword_recordings')
@patch('etl_lambda.fetch_subscription_data_from_rds')
def test_lambda_handler_exception(mock_fetch_rds, mock_clear_recs, caplog):
    """Test successful lambda handler function."""
    file_name = 'keyword_recording.csv'
    folder_name = 'long_term_keyword_data'
    bucket_name = 'test_bucket'

    mock_fetch_rds.side_effect = Exception()
    with caplog.at_level(logging.INFO):
        lambda_handler(MagicMock(), MagicMock())

    mock_fetch_rds.assert_called_once_with(
        UPDATE_QUERY, file_name, bucket_name, folder_name)
    mock_clear_recs.assert_not_called()

    assert f'Error processing {file_name}' in caplog.text
