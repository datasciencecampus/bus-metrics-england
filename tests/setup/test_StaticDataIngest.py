"""Tests for ingest_static_data module."""

import pytest
from bus_metrics.setup.ingest_static_data import StaticDataIngest
from datetime import datetime
import os

tool = StaticDataIngest()


def test_ingest_bus_timetable_file_exists():
    """Simple test to check that data not overwritten."""
    with pytest.raises(FileExistsError) as excinfo:
        date = datetime.now().strftime("%Y%m%d")
        open(f"tests/data/north_east_{date}.zip", "w")
        tool.zip_fp_root = "tests/data"
        tool.ingest_bus_timetable(region="north_east")
    assert (
        str(excinfo.value)
        == "The file you are downloading to already exists (timetable)"
    )
    os.remove(f"tests/data/north_east_{date}.zip")
