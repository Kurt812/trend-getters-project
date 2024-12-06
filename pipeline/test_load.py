"""Test script for load python file."""
# pylint: skip-file

import pytest
import os
import logging
import pandas as pd
import psycopg2
from unittest.mock import MagicMock, patch, mock_open
from psycopg2.extras import RealDictCursor
from load import (setup_connection, insert_keywords, insert_keyword_recordings,
                  insert_related_term_assignment, insert_related_terms, get_keyword_id, main)


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


@pytest.fixture()
def env(configs):
    """Fixture to mock environment variables."""
    with patch.dict(os.environ, configs):
        yield


@patch('load.psycopg2.connect')
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


@patch('load.psycopg2.connect')
def test_postgres_connection_operational_error(mock_connect, env, caplog):
    """Test unsuccessful connection to PostgreSQL due to OperationalError"""

    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn
    mock_connect.side_effect = psycopg2.OperationalError()

    with pytest.raises(psycopg2.OperationalError):
        setup_connection()
    assert 'Operational error while connecting to the database:' in caplog.text
    for record in caplog.records:
        assert record.levelname == 'ERROR'


@patch('load.psycopg2.connect')
def test_postgres_connection_exception(mock_connect, env, caplog):
    """Test general Exception raised when error setting up connection"""

    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn
    mock_cursor = MagicMock(spec=RealDictCursor)
    mock_conn.cursor.return_value = mock_cursor
    mock_cursor.execute.side_effect = Exception()

    with pytest.raises(Exception):
        setup_connection()
    assert 'Error connecting to database:' in caplog.text
    for record in caplog.records:
        assert record.levelname == 'ERROR'


@patch('load.setup_connection')
def test_successful_insert_keywords(mock_setup):
    """Test successful insertion of keywords into keywords table from a list of topics when they don't already exist."""
    mock_topics = ['python']

    mock_conn = MagicMock()
    mock_curs = MagicMock()
    mock_setup.return_value = (mock_conn, mock_curs)

    mock_curs.fetchone.return_value = None
    insert_query = "INSERT INTO keywords (keyword) VALUES (%s)"

    result = insert_keywords(mock_conn, mock_curs, mock_topics)
    mock_curs.execute.assert_called_with(insert_query, ('python',))
    assert result is None
    assert mock_conn.commit.call_count == 1


@patch('load.setup_connection')
def test_keyword_already_exists_no_insert(mock_setup):
    """Test case when the topic keyword already exists so is not inserted into db."""

    mock_topics = ['python']

    mock_conn = MagicMock()
    mock_curs = MagicMock()
    mock_setup.return_value = (mock_conn, mock_curs)

    mock_curs.fetchone.return_value = 1
    insert_keywords(mock_conn, mock_curs, mock_topics)
    assert mock_curs.execute.call_count == 1
    mock_conn.commit.assert_not_called()


@patch('load.setup_connection')
def test_insert_keyword_recordings_success(mock_setup):
    """Test successful insertion of data into keyword_recordings_table."""

    mock_conn = MagicMock()
    mock_curs = MagicMock()
    mock_setup.return_value = (mock_conn, mock_curs)

    mock_df = pd.DataFrame({
        'Hour': ['08:00', '09:00', '10:00'],
        'Count': [15, 22, 18],
        'Average Sentiment': [0.75, 0.60, 0.80],
        'keyword_id': [1, 2, 3]
    })

    insert_keyword_recordings(mock_conn, mock_curs, mock_df)
    assert mock_curs.execute.call_count == 3
    assert mock_conn.commit.call_count == 3
    mock_curs.execute.assert_any_call(
        """INSERT INTO keyword_recordings
                       (keywords_id, total_mentions, avg_sentiment, hour_of_day)
                       VALUES (%s, %s, %s, %s)""",
        (3, 18, 0.80, '10:00')
    )


@patch('load.setup_connection')
def test_successful_insert_related_terms(mock_setup):
    """Test related terms can be inputted to the correct table."""
    mock_conn = MagicMock()
    mock_curs = MagicMock()
    mock_setup.return_value = (mock_conn, mock_curs)

    mock_df = pd.DataFrame({
        'Keyword': ['Sleep'],
        'Related Terms': ['Nap, Sleeping, Bed, Night, Doze']
    })

    result = insert_related_terms(mock_conn, mock_curs, mock_df)
    assert isinstance(result, dict)
    assert mock_curs.fetchone.call_count == 5
    assert mock_curs.execute.call_count == 5
    assert mock_conn.commit.call_count == 5
    mock_curs.execute.assert_any_call(
        'SELECT related_term_id FROM related_terms\n                               WHERE related_term = %s', ('Nap',))


@patch('load.setup_connection')
def test_get_keyword_id(mock_setup):
    """Test successful retrieval of keyword_id"""
    keyword = 'python'
    mock_conn = MagicMock()
    mock_curs = MagicMock()
    mock_setup.return_value[0] = mock_conn
    mock_setup.return_value[1] = mock_curs

    mock_curs.fetchone.return_value = {'keywords_id': 24}
    result = get_keyword_id(mock_curs, keyword)
    mock_curs.execute.assert_called_with(
        """SELECT keywords.keywords_id FROM keywords WHERE keyword = %s""", (keyword, ))
    assert mock_curs.fetchone.call_count == 1
    assert result == 24


@patch('load.setup_connection')
def test_keyword_id_not_found(mock_setup, caplog):
    """Test error is raised when keyword id not found in RDS."""
    keyword = 'python'
    mock_conn = MagicMock()
    mock_curs = MagicMock()
    mock_setup.return_value[0] = mock_conn
    mock_setup.return_value[1] = mock_curs

    mock_curs.fetchone.return_value = None
    with caplog.at_level(logging.ERROR):
        with pytest.raises(ValueError):
            get_keyword_id(mock_curs, keyword)
    assert "Keyword 'python' not found in the database." in caplog.text


@patch('load.get_keyword_id', return_value=1)
@patch('load.setup_connection')
def test_insert_related_term_assignment(mock_setup, mock_get_id):
    """Test successful insert of data into related term assignment table."""
    mock_keywords_ids = {1: 'python'}

    mock_conn = MagicMock()
    mock_curs = MagicMock()
    mock_setup.return_value[0] = mock_conn
    mock_setup.return_value[1] = mock_curs

    insert_related_term_assignment(mock_conn, mock_curs, mock_keywords_ids)
    assert mock_curs.execute.call_count == 1
    assert mock_conn.commit.call_count == 1


@patch('load.get_keyword_id', return_value=1)
@patch('load.insert_related_term_assignment')
@patch('load.insert_related_terms')
@patch('load.insert_keyword_recordings')
@patch('load.insert_keywords')
@patch('load.setup_connection')
def test_main_success(mock_setup, mock_insert_keywords, mock_insert_recordings, mock_insert_related, mock_insert_assignment, mock_id, env, caplog):
    """Test the main load function of load will import data into RDS successfully."""
    mock_topics = ['python']
    mock_df = pd.DataFrame({
        'Hour': ['08:00', '09:00', '10:00'],
        'Count': [15, 22, 18],
        'Average Sentiment': [0.75, 0.60, 0.80],
        'keyword_id': [1, 2, 3]
    })
    mock_conn = MagicMock()
    mock_curs = MagicMock()
    mock_setup.return_value = (mock_conn, mock_curs)

    mock_related_ids = MagicMock()
    mock_insert_related.return_value = mock_related_ids

    main(mock_topics, mock_df)
    mock_insert_keywords.assert_called_once_with(
        mock_conn, mock_curs, mock_topics)

    mock_insert_recordings.assert_called_once_with(
        mock_conn, mock_curs, mock_df)
    mock_insert_related.assert_called_once_with(mock_conn, mock_curs, mock_df)
    mock_insert_assignment.assert_called_once_with(
        mock_conn, mock_curs, mock_related_ids)
