"""Tests for ingest_static_data module."""

import pytest
from src.bus_metrics.setup.ingest_static_data import StaticDataIngest
from datetime import datetime
import os


@pytest.fixture
def static_tool():
    """Load class to test."""
    return StaticDataIngest()


def test_ingest_bus_timetable_file_exists(static_tool):
    """Simple test to check that data not overwritten."""
    with pytest.raises(FileExistsError) as excinfo:
        date = str(datetime.now().date())
        open(f"tests/data/north_east_{date}.zip", "w")
        static_tool.zip_fp_root = "tests/data"
        static_tool.ingest_bus_timetable(region="north_east")
    assert (
        str(excinfo.value)
        == "The file you are downloading to already exists (timetable)"
    )
    os.remove(f"tests/data/north_east_{date}.zip")
