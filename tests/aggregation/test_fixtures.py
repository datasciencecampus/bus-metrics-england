"""A file sets up fixtures for tests."""

# from pyprojroot import here
import os
import pytest
import time
from src.bus_metrics.setup.ingest_static_data import StaticDataIngest
from src.bus_metrics.setup.ingest_realtime_data import RealtimeDataIngest
from src.bus_metrics.aggregation.build_schedules import Schedule_Builder
from datetime import datetime

stool = StaticDataIngest()
rtool = RealtimeDataIngest()
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
    gtfs_path = tmp_path_factory.mktemp("gtfs")
    stool.zip_fp_root = os.path.join(gtfs_path)

    try:
        stool.ingest_bus_timetable(region="north_east")
    except FileExistsError:
        print("GTFS already downloaded.")

    return gtfs_path


@pytest.fixture(scope="session")
def test_real_path(tmp_path_factory):
    """Downloads realtime to temp folder, returns path.

    Returns
    -------
    gtfs_path : str
        Path to the temporary folder which contains GTFS.

    """
    real_path = tmp_path_factory.mktemp("real")
    scriptStartTime = datetime.now()
    scriptStartTimeUnix = time.mktime(scriptStartTime.timetuple())

    while time.mktime(datetime.now().timetuple()) < scriptStartTimeUnix + 30:
        try:
            rtool.api_call()
            fileTimeStamp = datetime.now().strftime("%Y%m%d-%H:%M:%S")
            rtool.parse_realtime(
                filename=os.path.join(
                    real_path, f"north_east_{date}-{fileTimeStamp}.csv"
                )
            )
            time.sleep(10)
        except Exception as e:
            print(e)
            pass

    return real_path


@pytest.fixture(scope="module")
def config():
    """Create a template config for tests.

    Returns
    -------
    config : dict
        Example config dict using North East for today.

    """
    config_setup = {
        "region": "north_east",
        "date": str(date),
        "time_from": 7,
        "time_to": 10,
        "partial_timetable": False,
        "route_types": [3],
        "output_unlabelled_bulk": True,
    }

    return config_setup


@pytest.fixture(scope="module")
def stops_test(tmp_path_factory):
    """Download stop data to tmp_path_factory.

    Returns
    -------
    stop_path : str
        Path to gb_stops.csv

    """
    stop_path = os.path.join(
        f"{tmp_path_factory.getbasetemp()}", "gb_stops.csv"
    )
    stool.import_stops_from_naptan(filename=stop_path)

    return stop_path


@pytest.fixture(scope="module")
def test_class_instantiate(config, test_gtfs_path, test_real_path):
    """Test class for future tests.

    Returns
    -------
    builder : Schedule_builder
        Instantiated class.

    """
    class_input = config

    builder = Schedule_Builder(**class_input)
    builder.timetable_dir = test_gtfs_path
    builder.realtime_dir = test_real_path

    return builder
