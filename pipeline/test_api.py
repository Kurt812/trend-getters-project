"""Test file for api.py"""
# pylint: skip-file

import pytest
from unittest.mock import patch

from api import app, check_no_punctuation


@pytest.fixture
def test_api():
    "fake api."
    return app.test_client()


def test_successful_post_request(test_api):
    """Test successful post request yields expected results."""
    with patch('api.main') as mock_main:
        fake_data = {
            "topic_name": "test"
        }

        response = test_api.post('/topics', json=fake_data)
        mock_main.assert_called_once_with(['test'])
        assert response.status_code == 200
        assert response.json['message'] == 'Topic added successfully'
        assert response.json['topic'] == {'topic_name': 'test'}


def test_unsuccessful_post_missing_topic(test_api):
    """Test suitable error code and message is displayed if no topic name is provided."""
    fake_data = {}
    response = test_api.post('/topics', json=fake_data)
    assert response.status_code == 400
    assert response.json['message'] == 'Topic name is required'


def test_more_than_one_word_topic(test_api):
    """Test scenario when user-submitted topic contains more than one word."""

    with patch('api.main') as mock_main:
        fake_data = {
            "topic_name": "testing this"
        }
        response = test_api.post('/topics', json=fake_data)
        mock_main.assert_called_once_with(['testing this'])
        assert response.status_code == 200
        assert response.json['message'] == 'Topic added successfully'
        assert response.json['topic'] == {'topic_name': 'testing this'}


def test_punctuation_is_found():
    """Test if user enters in punctuation, then it is found in this function."""
    topic = "Hello! World %^*"
    result = check_no_punctuation(topic)
    assert result is True


@patch('api.main')
def test_punctuation_returns_error(mock_main, test_api):
    """Test if user enters in punctuation then error message is returned."""
    fake_data = {
        "topic_name": "Hello! World %^*"
    }
    response = test_api.post('/topics', json=fake_data)
    assert response.status_code == 400
    assert response.json['message'] == 'No punctuation permitted in topic.'
    mock_main.assert_not_called()
