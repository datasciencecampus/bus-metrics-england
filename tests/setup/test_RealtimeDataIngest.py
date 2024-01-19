"""Tests for ingest_realtime_data module."""

import pytest
from config import BODS_API_KEY, test_BODS_API_KEY
from src.setup.ingest_realtime_data import RealtimeDataIngest

tool = RealtimeDataIngest()


def test_error_API_failure():
    """Simple test to check raise error if API key faulty."""
    # TODO: check multiple warnings raised
    tool.api_key = test_BODS_API_KEY
    with pytest.raises(AttributeError) as excinfo:
        tool._api_call()
    assert type(excinfo.value) == AttributeError


def test_csv_storage_success(tmp_path):
    """Simple test to check that csv is written."""
    temp_file = tmp_path / "test.csv"
    # required as earlier test tests different key
    tool.api_key = BODS_API_KEY
    tool.parse_realtime(filename=temp_file)
    assert temp_file.is_file()
