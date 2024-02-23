"""Tests for bus_metric/aggregation/preproccessing file."""
import os
import pytest
from datetime import datetime
from src.bus_metrics.aggregation.preprocessing import unzip_GTFS

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
