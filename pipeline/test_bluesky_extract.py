"""Test script for BlueSky extract python file."""

import logging
import pytest
import json
from unittest.mock import MagicMock, patch

from bluesky_extract import (format_text, extract_text_from_bytes, JSONExtra,
                             get_firehose_data)


def test_correct_formatting():
    """Test the function removes all extra new lines and whitespaces successfully."""
    test_string = "H ello    \n \n \n Good bye!"
    assert format_text(test_string) == "H ello Good bye!"


@patch('bluesky_extract.json')
def test_successful_extract_text(mock_json):
    """Parsed = dict, json_data=str, text=str"""
    """Test that bytes are successfully parsed into dict then strings."""

    raw_byte_input = b'{"text": "Hello World", "$type": "app.bsky.feed.post", "langs": ["en"], "createdAt": "2024-12-03T11:17:35.355Z"}'

    mock_json.dumps.return_value = '{"text": "Hello World", "$type": "app.bsky.feed.post", "langs": ["en"], "createdAt": "2024-12-03T11:17:35.355Z"}'
    mock_json.loads.return_value = {"text": "Hello World", "$type": "app.bsky.feed.post", "langs": [
        "en"], "createdAt": "2024-12-03T11: 17: 35.355Z"}
    result = extract_text_from_bytes(raw_byte_input)
    assert result == "Hello World"
    assert isinstance(result, str)


@patch('bluesky_extract.json')
def test_unsuccessful_extract_text_typeerror(mock_json, caplog):
    """Test a type error will be raised with str input instead of bytes."""

    raw_byte_input = '{"text": "Hello World", "$type": "app.bsky.feed.post", "langs": ["en"], "createdAt": "2024-12-03T11:17:35.355Z"}'

    mock_json.dumps.side_effect = TypeError(
        "Simulated json dumps error")
    with caplog.at_level(logging.WARNING):
        extract_text_from_bytes(raw_byte_input)

    assert "Error extracting text:" in caplog.text
    mock_json.loads.assert_not_called()


@patch('bluesky_extract.json')
@patch('bluesky_extract.format_text')
def test_unsuccessful_extract_text_attributeerror(mock_format_text, mock_json, caplog):
    """Parsed = dict, json_data=str, text=str"""
    """Test function to ensure an attribute error is logged if str cannot be parsed."""

    raw_byte_input = b'["text": "Hello World", "$type": "app.bsky.feed.post", "langs": ["en"], "createdAt": "2024-12-03T11:17:35.355Z"]'
    mock_json.dumps.return_value = '["text": "Hello World", "$type": "app.bsky.feed.post", "langs": ["en"], "createdAt": "2024-12-03T11:17:35.355Z"]'
    mock_json.loads.side_effect = AttributeError(
        "Simulated json dumps error")
    with caplog.at_level(logging.WARNING):
        extract_text_from_bytes(raw_byte_input)

    assert "Error extracting text:" in caplog.text
    mock_format_text.assert_not_called()


def test_firehose_invalid_commit_type():
    """Test that if it is not a commit (i.e. an upload)"""
