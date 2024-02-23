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


def test_load_raw_realtime_data(test_class_instantiate, config):
    """PROPOSED UNIT TEST: Simple test to check loading of realtime data."""
    test_region = config["region"]
    test_date = "20240221"
    test_class_instantiate.realtime_dir = "tests/data"
    real_df = test_class_instantiate.load_raw_realtime_data(
        test_region, test_date
    )
    assert type(real_df) == pl.DataFrame


def test_split_realtime_data(test_class_instantiate):
    """Simple test to check splitting of realtime data."""
    # Load sample DataFrame.
    sample_df = pl.read_csv("tests/data/north_east_20240221-SAMPLE.csv")
    print(sample_df.head(1))

    # Add unlablled row.
    new_row = pl.DataFrame(
        {
            "time_ingest": 1708515464,
            "time_transpond": 1708515464,
            "bus_id": 7357,
            "trip_id": None,
            "route_id": None,
            "current_stop": 0,
            "latitude": 51.478916,
            "longitude": -0.090233,
            "bearing": 0,
            "journey_date": 20240221,
        },
        schema={
            "time_ingest": pl.Int64,
            "time_transpond": pl.Int64,
            "bus_id": pl.Int64,
            "trip_id": pl.Utf8,
            "route_id": pl.Int64,
            "current_stop": pl.Int64,
            "latitude": pl.Float64,
            "longitude": pl.Float64,
            "bearing": pl.Float64,
            "journey_date": pl.Int64,
        },
    )
    print(new_row.head())

    sample_df = pl.concat([sample_df, new_row])

    # Use class mathod to test splitting of data.
    labelled, unlalebbed = test_class_instantiate.split_realtime_data(
        sample_df
    )

    assert len(labelled) == 6
    assert len(unlalebbed) == 1
