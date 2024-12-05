"""Test file for api.py"""
# pylint: skip-file

import pytest
from unittest.mock import patch

from api import app


@pytest.fixture
def test_api():
    "fake api."
    return app.test_client()


def test_successful_post_request(test_api):
    """Test successful post request yields expected results."""
    with patch('api.main') as mock_main:
        fake_data = {
            'topic_name': 'test'
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


# def test_add_multiple_topics(client):
#     data_1 = {"topic_name": "Science"}
#     data_2 = {"topic_name": "Art"}

#     response_1 = client.post("/topics", json=data_1)
#     response_2 = client.post("/topics", json=data_2)

#     assert response_1.status_code == 200
#     assert response_2.status_code == 200
#     assert len(topics) == 2
#     assert topics[0]["topic_name"] == "Science"
#     assert topics[1]["topic_name"] == "Art"
