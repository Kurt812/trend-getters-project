"""Test file for etl.py: Moving data from RDS to S3."""
# pylint: skip-file

import pytest
import os
import logging
import pandas as pd
import psycopg2
from unittest.mock import MagicMock, patch
from psycopg2.extras import RealDictCursor

from etl import (setup_connection, s3_connection, download_csv_from_s3,
                 upload_to_s3, delete_local_file, fetch_subscription_data_from_rds, main)


@pytest.fixture()
def configs():
    """Configs for .env file"""
    return {
        "DB_USERNAME": "user",
        "DB_PASSWORD": "password",
        "DB_HOST": "localhost",
        "DB_PORT": "1234",
        "DB_NAME": "name",
        "AWS_ACCESS_KEY_ID": "fake_access_key",
        "AWS_SECRET_ACCESS_KEY": "fake_secret_key",
        "SCHEMA_NAME": "fake_schema"
    }


@pytest.fixture(autouse=True)
def env(configs):
    """Fixture to mock environment variables."""
    with patch.dict(os.environ, configs):
        yield


@patch('etl.psycopg2.connect')
def test_setup_connection_success(mock_connect, env, caplog):
    """Test successful connection and schema setting."""

    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn
    mock_cursor = MagicMock(spec=RealDictCursor)
    mock_conn.cursor.return_value = mock_cursor

    with caplog.at_level(logging.INFO):
        result = setup_connection()
    assert result == (mock_conn, mock_cursor)
    assert 'Connection successfully established to database.' in caplog.text
    mock_cursor.execute.assert_called_once()
