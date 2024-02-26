"""Tests for bus_metric/aggregation/preproccessing file."""
import os
import pytest
import polars as pl
from datetime import datetime
from src.bus_metrics.aggregation.preprocessing import (
    unzip_GTFS,
    polars_robust_load_csv,
    convert_unix_to_time_string,
    build_stops,
)

pytest_plugins = ["tests.aggregation.test_fixtures"]

date = datetime.now().strftime("%Y%m%d")


def test_unzip(tmp_path, txt_files, test_gtfs_path):
    """Simple test to specifically test that the .zip file unzips correctly."""
    temp = tmp_path
    unzip_GTFS(
        txt_path=temp,
        zip_path=test_gtfs_path,
        file_name_pattern=f"{date}.zip",
        logger=None,  # noqa:E501
    )
    contents1 = set(os.listdir(temp))
    contents2 = set(txt_files)

    assert (
        contents1 == contents2
    ), f"""Directory contents are different: {
        contents1 - contents2}, {contents2 - contents1}"""


def test_unzip_file_checking(tmp_path, txt_files, test_gtfs_path):
    """A test to check that the unzip function.

    A check to see missing files, and subsequently
    unzip the .zip to provide missing files.

    """
    temp = tmp_path
    for i in txt_files[:-2]:
        with open(f"{temp}{i}", "w") as file:  # noqa:F841
            pass
    unzip_GTFS(
        txt_path=temp,
        zip_path=test_gtfs_path,
        file_name_pattern=f"{date}.zip",
        logger=None,  # noqa:E501
    )
    files_in_dir = os.listdir(temp)
    files_in_dir = set([file.split("/")[-1] for file in files_in_dir])
    assert (
        set(txt_files) == files_in_dir
    ), f"""Directory contents are different: {
        set(txt_files) - files_in_dir}, {files_in_dir - set(txt_files)}"""


def test_multiple_zips(tmp_path):
    """A test to check when 2 zips are found.

    Test functionality if there are 2 .zip files in the zip_path
    directory, the unzip_GTFS function raises correct error.

    """
    with pytest.raises(
        ValueError, match="More than 1 file_name_pattern in zip_path"
    ):  # noqa:E501
        for i in range(2):
            with open(
                os.path.join(tmp_path, f"{i}temp_timetable.zip"), "w"
            ) as file:  # noqa: F841
                pass
        unzip_GTFS(tmp_path, tmp_path, "timetable.zip", None)


def test_no_zips(tmp_path):
    """A test to check when no zips found.

    A check that if there is no .zip file matching zip
    pattern in a specified path it raises the correct error.

    """
    with pytest.raises(ValueError, match="No"):
        unzip_GTFS(tmp_path, tmp_path, ".zip", None)


def test_robust_load_csv():
    """Simple test to check loading of csv with polars."""
    # Test case when dtypes is None
    test_csv = "tests/data/north_east_20240221-SAMPLE.csv"
    df = polars_robust_load_csv(test_csv)
    assert isinstance(df, pl.DataFrame)
    assert df.shape == (6, 10)


def test_polars_dtypes():
    """Test to check assignment of dtype to dataframe."""
    # Test case when dtypes is Specified
    test_csv = "tests/data/north_east_20240221-SAMPLE.csv"
    df = polars_robust_load_csv(test_csv, dtypes={"route_id": pl.Utf8})
    assert isinstance(df, pl.DataFrame)
    assert df["route_id"].dtype == pl.Utf8


def test_convert_unix_to_time_string():
    """Test to check conversion of unix timestamp to string."""
    # Read in sample csv
    sample_dataframe = pl.read_csv("tests/data/north_east_20240221-SAMPLE.csv")

    # Apply function to the sample DataFrame
    df_result = convert_unix_to_time_string(
        sample_dataframe, unix_column="time_transpond"
    )

    # Check if the new column is added
    assert "dt_time_transpond" in df_result.columns

    # Check if the datatype of the new column is time
    assert df_result["dt_time_transpond"].dtype == pl.Time

    # Assign expected values
    expected_values = [
        "11:37:06",
        "11:37:16",
        "11:37:16",
        "11:36:51",
        "11:37:12",
        "11:37:06",
    ]

    # Check if the conversion is accurate
    assert (
        df_result["dt_time_transpond"]
        .apply(lambda x: x.strftime("%H:%M:%S"))
        .to_list()
        == expected_values
    )


def test_build_stops():
    """Simple test checking processed stops data."""
    output = build_stops(
        output="polars", stops_data="tests/data/stops_sample.csv"
    )
    assert type(output) == pl.DataFrame
    assert output.columns == ["stop_id", "stop_lat", "stop_lon"]
    assert output[0, "stop_lat"] == 51.44902101682718
    assert output[0, "stop_lon"] == -2.5857890312830363
