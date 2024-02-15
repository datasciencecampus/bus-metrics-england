"""A file sets up fixtures for tests."""

# from pyprojroot import here
import os
import pytest
from bus_metrics.setup.ingest_static_data import StaticDataIngest
from datetime import datetime

tool = StaticDataIngest()
date = datetime.now().strftime("%Y%m%d")

###########
# fixures #
###########


# returns list of mandatory .txt files in gtfs.
@pytest.fixture(scope="module")
def txt_files():
    """Return a list of text files which should be in the GTFS zip file.

    Returns
    -------
    list
        list of text files unzipped from gtfs.

    """
    files_to_check = [
        "agency.txt",
        "calendar.txt",
        "calendar_dates.txt",
        "feed_info.txt",
        "routes.txt",
        "shapes.txt",
        "stop_times.txt",
        "stops.txt",
        "trips.txt",
    ]

    return files_to_check


@pytest.fixture(scope="session")
def test_gtfs_path(tmp_path_factory):
    """Downloads test GTFS to temp folder, returns path.

    Returns
    -------
    gtfs_path : str
        Path to the temporary folder which contains GTFS.

    """
    gtfs_path = os.path.join(tmp_path_factory.getbasetemp(), "gtfs")

    if not os.path.exists(gtfs_path):
        # If it doesn't exist, create the folder
        os.makedirs(gtfs_path)

    # set download path
    tool.zip_fp_root = os.path.join(gtfs_path)

    try:
        tool.ingest_bus_timetable(region="north_east")
    except FileExistsError:
        print("GTFS already downloaded.")

    return gtfs_path
