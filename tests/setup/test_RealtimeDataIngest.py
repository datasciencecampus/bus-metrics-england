"""Tests for ingest_realtime_data module."""

import pytest
from src.bus_metrics.setup.ingest_realtime_data import RealtimeDataIngest
from google.transit import gtfs_realtime_pb2
from google.protobuf import text_format


@pytest.fixture
def rt_tool():
    """Load class to test."""
    return RealtimeDataIngest()


def test_error_API_failure(rt_tool):
    """Simple test to check raise error if API key faulty."""
    # TODO: check multiple warnings raised
    rt_tool.api_key = "faulty_api_key"  # pragma: allowlist secret
    with pytest.raises(AttributeError) as excinfo:
        rt_tool._api_call()
    assert type(excinfo.value) == AttributeError


def _parse_protobuf_message_entity(file_path):
    """Read test BODS message (HELPER)."""
    with open(file_path, "r") as f:
        return text_format.Parse(f.read(), gtfs_realtime_pb2.FeedMessage())


def test_csv_storage_success(rt_tool, tmp_path):
    """Simple test to check that csv is written."""
    # TODO: check multiple warnings raised
    temp_file = tmp_path / "test.csv"
    # inject test BODS message
    rt_tool.message = _parse_protobuf_message_entity(
        "tests/data/BODS_API_message"
    )
    rt_tool.parse_realtime(filename=temp_file)
    assert temp_file.is_file()
