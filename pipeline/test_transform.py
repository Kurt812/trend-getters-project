"""Test for transform.py"""
# pylint: skip-file

import pytest
import os
import psycopg2
from unittest.mock import MagicMock, patch

from transform import (get_connection)


@patch('transform.psycopg2.connect')
def test_postgres_connection_success(mock_connect, caplog):
    """Test successful connection to PostgreSQL using patch and MagicMock."""

    configs = {
        "DB_USERNAME": "user",
        "DB_PASSWORD": "password",
        "DB_HOST": "localhost",
        "DB_PORT": 1234,
        "DB_NAME": "name"
    }
    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn
    with caplog.at_level('INFO'):
        conn = get_connection(configs)

    mock_connect.assert_called_once_with(
        user="user", password='password', host="localhost", port=1234, database="name")

    assert conn == mock_conn
    assert 'Connection successfully established to database.' in caplog.text


@patch('transform.psycopg2.connect')
def test_postgres_connection_operational_error(mock_connect, caplog):
    """Test unsuccessful connection to PostgreSQL due to OperationalError"""

    configs = {
        "DB_USERNAME": "user",
        "DB_PASSWORD": "password",
        "DB_HOST": "localhost",
        "DB_PORT": 1234,
        "DB_NAME": "name"
    }
    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn
    mock_connect.side_effect = psycopg2.OperationalError()

    with pytest.raises(psycopg2.OperationalError):
        get_connection(configs)
    assert 'Operational error while connecting to the database:' in caplog.text
    for record in caplog.records:
        assert record.levelname == 'ERROR'


@patch('')
def test_get_cursor():
    """Test cursor is successfully returned from a connection."""
