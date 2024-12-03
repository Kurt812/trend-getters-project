"""Test script for BlueSky extract python file."""


import pytest


def test_extract_botanist_information():
    """Tests extracts botanists information successfully """
    input_data = {"name": "Test Test",
                  "email": "test@test.com", "phone": "+0000 111222"}
    output_data = {"name": "Test Test",
                   "email": "test@test.com", "phone": "+0000 111222"}
