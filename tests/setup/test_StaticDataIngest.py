"""Tests for ingest_static_data module."""

import pytest
from src.bus_metrics.setup.ingest_static_data import StaticDataIngest
from datetime import datetime
import os

tool = StaticDataIngest()
TESTS_DATA_PATH = os.path.join("tests", "data")


def test_ingest_bus_timetable_file_exists():
    """Simple test to check that data not overwritten."""
    with pytest.raises(FileExistsError) as excinfo:
        date = datetime.now().strftime("%Y%m%d")
        with open(
            os.path.join(TESTS_DATA_PATH, f"north_east_{date}.zip"), "w"
        ) as f:
            f.write("")
        tool.zip_fp_root = TESTS_DATA_PATH
        tool.ingest_bus_timetable(region="north_east")
    assert (
        str(excinfo.value)
        == "The file you are downloading to already exists (timetable)"
    )
    os.remove(os.path.join(TESTS_DATA_PATH, f"north_east_{date}.zip"))


def test_ingest_data_from_geoportal_path_exists():
    """Simple test to check abort if file exists locally."""
    with pytest.raises(FileExistsError) as excinfo:
        filename = "tests/data/LAD_boundaries.geojson"
        tool.ingest_data_from_geoportal(filename=filename)
    assert (
        str(excinfo.value)
        == "The file you are downloading to already exists (bounds)"
    )
