"""Tests for Schedule_builder class."""
import polars as pl

pytest_plugins = ["tests.aggregation.test_fixtures"]


def test_build_timetable_func(stops_test, test_class_instantiate, config):
    """Simple test to check reading in of timetable data."""
    test_stops = pl.read_csv(
        stops_test,
        ignore_errors=True,
        dtypes={"stop_id": pl.Utf8},  # noqa: E501
    )

    test_stops = test_stops[["ATCOCode", "Latitude", "Longitude"]]
    test_stops.columns = ["stop_id", "stop_lat", "stop_lon"]
    test_region = config["region"]
    test_date = config["date"]
    timetable_df = test_class_instantiate.build_timetable(
        stops=test_stops, region=test_region, date=test_date
    )
    assert (type(timetable_df) == pl.DataFrame) & (
        len(timetable_df) > 1
    ), "Timetable Data not read in."


def test_build_realtime_func(test_class_instantiate, config):
    """Simple test to check loading of realtime data."""
    test_region = config["region"]
    test_date = config["date"]
    real_df, _ = test_class_instantiate.build_realtime(test_region, test_date)
    assert type(real_df) == pl.DataFrame


def test_load_raw_realtime_data(test_class_instantiate, config):
    """PROPOSED UNIT TEST: Simple test to check loading of realtime data."""
    test_region = config["region"]
    test_date = config["date"]
    real_df = test_class_instantiate.load_raw_realtime_data(
        test_region, test_date
    )
    assert type(real_df) == pl.DataFrame
