"""Tests for ingest_static_data module."""

import pytest
from src.setup.ingest_static_data import StaticDataIngest

tool = StaticDataIngest()


def test_ingest_bus_timetable_file_exists():
    """Simple test to check that data not overwritten."""
    with pytest.raises(FileExistsError) as excinfo:
        tool.timetable_url_prefix = "tests/data"
        tool.ingest_bus_timetable(region="north_east")
    assert (
        str(excinfo.value)
        == "The file you are downloading to already exists (timetable)"
    )
