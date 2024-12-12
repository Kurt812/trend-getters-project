"""Test for transform.py"""
# pylint: skip-file

import logging
import pytest
import pandas as pd
import psycopg2
from unittest.mock import MagicMock, patch, mock_open
from psycopg2.extras import RealDictCursor
from transform import (get_connection, get_cursor,
                       ensure_keywords_in_db, keyword_matching, extract_keywords_from_csv, main)


@patch('transform.psycopg2.connect')
@patch.dict('os.environ', {
    "DB_USERNAME": "user",
    "DB_PASSWORD": "password",
    "DB_HOST": "localhost",
    "DB_PORT": "1234",
    "DB_NAME": "name"
})
def test_postgres_connection_success(mock_connect, caplog):
    """Test successful connection to PostgreSQL using patch and MagicMock."""

    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn
    with caplog.at_level('INFO'):
        conn = get_connection()

    mock_connect.assert_called_once_with(
        user="user", password='password', host="localhost", port=1234, database="name")

    assert conn == mock_conn
    assert 'Connection successfully established to database.' in caplog.text


@patch('transform.psycopg2.connect')
def test_postgres_connection_operational_error(mock_connect, caplog):
    """Test unsuccessful connection to PostgreSQL due to OperationalError"""

    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn
    mock_connect.side_effect = psycopg2.OperationalError()

    with pytest.raises(psycopg2.OperationalError):
        get_connection()
    assert 'Operational error while connecting to the database:' in caplog.text
    for record in caplog.records:
        assert record.levelname == 'ERROR'


def test_get_cursor():
    """Test cursor is successfully returned from a connection."""
    mock_connection = MagicMock()
    mock_cursor = MagicMock(spec=RealDictCursor)
    mock_connection.cursor.return_value = mock_cursor

    result = get_cursor(mock_connection)
    mock_connection.cursor.assert_called_once_with(
        cursor_factory=RealDictCursor)
    assert result == mock_cursor


@patch('transform.get_connection')
@patch('transform.get_cursor')
def test_successful_ensure_keywords_in_db(mock_conn, mock_curs):
    """Test that all words given are already in db"""
    mock_keywords = ['hello', 'goodbye']

    mock_conn.cursor.return_value = mock_curs
    mock_curs.fetchall.return_value = [
        {'keyword': 'hello', 'keywords_id': 1},
        {'keyword': 'goodbye', 'keywords_id': 2},
        {'keyword': 'flower', 'keywords_id': 3}
    ]
    ensure_keywords_in_db(mock_keywords, mock_curs, mock_conn)
    mock_curs.execute.call_count == 2
    mock_conn.commit.assert_not_called()
    mock_curs.fetchon.assert_not_called()


@patch('transform.get_connection')
@patch('transform.get_cursor')
def test_add_missing_words_to_db(mock_conn, mock_curs):
    """Test that words in keywords that aren't already in db are entered in."""
    mock_keywords = ['cactus']

    mock_conn.cursor.return_value = mock_curs
    mock_curs.fetchall.return_value = [
        {'keyword': 'hello', 'keywords_id': 1},
        {'keyword': 'goodbye', 'keywords_id': 2},
        {'keyword': 'flower', 'keywords_id': 3}
    ]

    mock_curs.fetchone.return_value = {'keyword': 'cactus', 'keywords_id': 4}

    expected_result = {'cactus': 4, 'flower': 3, 'goodbye': 2, 'hello': 1}
    result = ensure_keywords_in_db(mock_keywords, mock_curs, mock_conn)
    mock_curs.execute.call_count == 3
    mock_conn.commit.assert_called_once()
    assert result == expected_result


@patch('transform.get_connection')
@patch('transform.get_cursor')
def test_no_words_in_db(mock_conn, mock_curs):
    """Test if no words are in the db, the word will still be added in gracefully."""
    mock_keywords = ['cactus']

    mock_conn.cursor.return_value = mock_curs
    mock_curs.fetchall.return_value = []

    mock_curs.fetchone.return_value = {'keyword': 'cactus', 'keywords_id': 1}

    expected_result = {'cactus': 1}
    result = ensure_keywords_in_db(mock_keywords, mock_curs, mock_conn)
    mock_curs.execute.call_count == 3
    mock_conn.commit.assert_called_once()
    assert result == expected_result


def test_keyword_matching_successful():
    """Test that keyword_id is successfully assigned based on matching keywords in content."""
    mock_df = pd.DataFrame(
        {'Keyword': ['cactus', 'flower', 'goodbye', 'hello']})
    mock_keyword_map = {'cactus': 4, 'flower': 3, 'goodbye': 2, 'hello': 1}
    expected_df = pd.DataFrame(
        {'Keyword': ['cactus', 'flower', 'goodbye', 'hello'],
         'keyword_id': [4, 3, 2, 1]}).astype({'keyword_id': 'object'})

    result = keyword_matching(mock_df, mock_keyword_map)
    pd.testing.assert_frame_equal(result, expected_df)


def fake_data():
    """Fake csv data for testing."""
    return "Keyword,keyword_id\nhello,1\ngoodbye,2\nflower,3\ncactus,4\n"


@patch('builtins.open', new_callable=mock_open, read_data=fake_data())
@patch('os.path.isfile', return_value=True)
def test_extract_keywords_from_csv_success(mock_isfile, mock_open):
    """Test that keywords can be extracted successfully from a mock csv file."""
    expected_result = ['cactus', 'flower', 'goodbye', 'hello']
    result = extract_keywords_from_csv('mock_file.csv')
    mock_open.assert_called_once_with(
        'mock_file.csv', 'r', encoding='utf-8', errors='strict', newline='')
    assert sorted(result) == sorted(expected_result)


@patch('os.path.isfile', return_value=True)
def test_no_csv_to_extract_from(mock_isfile, caplog):
    """Test case when there is no csv to extract keywords from."""
    with caplog.at_level(logging.ERROR):
        with pytest.raises(FileNotFoundError):
            extract_keywords_from_csv('mock_file.csv')
    assert 'File not found at path mock_file.csv' in caplog.text


@patch('os.path.isfile', return_value=True)
@patch('builtins.open', new_callable=mock_open, read_data=fake_data())
def test_extract_keywords_exception(mock_open, mock_isfile, caplog):
    """Test that a general exception will be raised for any issues during function execution."""
    mock_open.side_effect = Exception('Test Exception')
    with caplog.at_level(logging.ERROR):
        with pytest.raises(Exception):
            extract_keywords_from_csv('mock_file.csv')
    assert 'An error occurred while reading the file ' in caplog.text


@patch('transform.get_connection')
@patch('transform.get_cursor')
@patch('transform.ensure_keywords_in_db')
@patch('transform.keyword_matching')
def test_main_success(mock_keyword_match, mock_ensure, mock_get_curs, mock_get_conn, caplog):
    """Test that the main function functions well and returns expected dataframe."""

    mock_df = pd.DataFrame({
        'Keyword': ['cactus', 'flower', 'goodbye', 'hello'],
    })

    mock_conn = MagicMock()
    mock_get_conn.return_value = mock_conn
    mock_curs = MagicMock(spec=RealDictCursor)
    mock_get_curs.return_value = mock_curs
    mock_conn.cursor.return_value = mock_curs

    mock_ensure.return_value = {
        'cactus': 4, 'flower': 3, 'goodbye': 2, 'hello': 1}

    expected_df = pd.DataFrame(
        {'Keyword': ['cactus', 'flower', 'goodbye', 'hello'],
         'keyword_id': [4, 3, 2, 1]}).astype({'keyword_id': 'object'})

    mock_keyword_match.return_value = expected_df

    with caplog.at_level(logging.INFO):
        result = main(mock_df)
    assert sorted(result) == sorted(expected_df)

    assert 'Connecting to the trends RDS' in caplog.text

    mock_get_conn.assert_called_once()
    mock_get_curs.assert_called_once()
    mock_ensure.assert_called_once_with(
        ['cactus', 'flower', 'goodbye', 'hello'], mock_curs, mock_conn)
    mock_keyword_match.assert_called_once_with(
        mock_df, {'cactus': 4, 'flower': 3, 'goodbye': 2, 'hello': 1})
