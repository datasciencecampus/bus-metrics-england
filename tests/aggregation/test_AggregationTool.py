"""Tests for punctuality_rate module."""

import pytest
import pandas as pd
from src.bus_metrics.aggregation.punctuality_rate import AggregationTool


@pytest.fixture
def aggregation_tool():
    """Load class to test."""
    return AggregationTool()


def test_merge_geographies_with_stop_punctuality(aggregation_tool):
    """Test punctuality and geographies merge correctly by stop."""
    aggregation_tool.stop_level_punctuality = (
        "tests/data/stop_level_punctuality.csv"
    )
    aggregation_tool.geography_lookup_table = (
        "tests/data/geography_lookup_table.csv"
    )
    result = aggregation_tool.merge_geographies_with_stop_punctuality()
    expected_dims = (3, 15)
    # test: output is dataframe
    assert isinstance(result, pd.DataFrame)
    # test: output dims (more secure than changeable geog versions)
    assert result.shape == expected_dims


def test_merge_geographies_with_stop_punctuality_file_not_found(
    aggregation_tool,
):
    """Test error returned when file missing."""
    aggregation_tool.stop_level_punctuality = "file_not_exists.csv"
    with pytest.raises(FileNotFoundError):
        aggregation_tool.merge_geographies_with_stop_punctuality()


def test_reaggregate_punctuality(aggregation_tool):
    """Test correct reaggregation of punctuality by geography."""
    aggregation_tool.code = "LSOA21CD"
    aggregation_tool.name = "LSOA21NM"
    labelled = pd.DataFrame(
        {
            "stop_id": [1, 2, 3],
            "service_stops": [10, 20, 30],
            "punctuality_rate": [0.8, 0.9, 0.7],
            aggregation_tool.name: ["A", "B", "A"],
            aggregation_tool.code: ["1", "2", "1"],
        }
    )
    expected_columns = [
        "LSOA21CD",
        "LSOA21NM",
        "service_stops",
        "punctual_service_stops",
        "punctuality_rate",
    ]
    expected_data = [["1", "A", 40, 29, 0.725], ["2", "B", 20, 18, 0.9]]

    result = aggregation_tool._reaggregate_punctuality(labelled)
    # test: correct format
    assert isinstance(result, pd.DataFrame)
    # test: correct columns
    assert list(result.columns) == expected_columns
    # test: correct output values
    assert result.values.tolist() == expected_data


# note use of mocker from pytest-mock
def test_punctuality_by_geography(aggregation_tool, mocker, tmp_path):
    """Test correct data format returned and csv stored."""
    aggregation_tool.outdir = tmp_path
    aggregation_tool.geography = "lsoa"
    temp_file = tmp_path / f"{aggregation_tool.geography}.csv"
    mocker.patch.object(
        aggregation_tool,
        "merge_geographies_with_stop_punctuality",
        return_value=pd.DataFrame(),
    )
    mocker.patch.object(
        aggregation_tool,
        "_reaggregate_punctuality",
        return_value=pd.DataFrame(),
    )
    result = aggregation_tool.punctuality_by_geography()
    # test: correct format
    assert isinstance(result, pd.DataFrame)
    # test: csv stored correctly
    assert temp_file.is_file()
