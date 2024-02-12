"""Tests for punctuality_rate module."""

import pytest
import pandas as pd
from src.bus_metrics.aggregation.punctuality_rate import AggregationTool
from numpy import nan  # req'd to avoid consecutive ,,, in test data


@pytest.fixture
def aggregation_tool():
    """Load class to test."""
    return AggregationTool()


@pytest.mark.skip(reason="not sure how to implant pre-read csv")
def test_merge_geographies_with_stop_punctuality(aggregation_tool):
    """Test punctuality and geographies merge correctly by stop."""
    stops = pd.DataFrame(  # noqa: F841
        {
            "stop_id": ["0100BRP90310", "490005344S", "3200YND57040"],
            "stop_lat": [
                51.44902101682718,
                51.55653865410456,
                53.98630052493483,
            ],
            "stop_lon": [
                -2.5857890312830363,
                -0.4733171011848141,
                -1.52401954306945,
            ],
            "service_stops": [17, 23, 4],
            "punctuality_rate": [0.17, 0.45, 1],
        }
    )
    lookup = [  # noqa: F841
        [
            "0100BRP90310",
            51.44902101682718,
            -2.5857890312830363,
            "E01033353",
            "Bristol 054B",
            nan,
            nan,
            "E06000023",
            "Bristol, City of",
            "E12000009",
            "South West",
            "E30000180",
            "Bristol",
        ],
        [
            "3200YND57040",
            53.98630052493483,
            -1.52401954306945,
            "E01027733",
            "Harrogate 017F",
            "E63000601",
            "Harrogate",
            "E06000065",
            "North Yorkshire",
            "E12000003",
            "Yorkshire and The Humber",
            "E30000214",
            "Harrogate",
        ],
        [
            "490005344S",
            51.55653865410456,
            -0.4733171011848141,
            "E01002512",
            "Hillingdon 013C",
            nan,
            nan,
            "E09000017",
            "Hillingdon",
            "E12000007",
            "London",
            "E30000266",
            "Slough and Heathrow",
        ],
    ]
    result = aggregation_tool._merge_geographies_with_stop_punctuality()
    assert isinstance(result, pd.DataFrame)


def test_merge_geographies_with_stop_punctuality_file_not_found(
    aggregation_tool,
):
    """Test error returned when file missing."""
    aggregation_tool.stop_level_punctuality = "file_not_exists.csv"
    with pytest.raises(FileNotFoundError):
        aggregation_tool._merge_geographies_with_stop_punctuality()


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
    assert isinstance(result, pd.DataFrame)
    assert list(result.columns) == expected_columns
    assert result.values.tolist() == expected_data


# note use of mocker from pytest-mock
def test_punctuality_by_geography(aggregation_tool, mocker):
    """Test correct data format returned."""
    aggregation_tool.geography = "lsoa"
    mocker.patch.object(
        aggregation_tool,
        "_merge_geographies_with_stop_punctuality",
        return_value=pd.DataFrame(),
    )
    mocker.patch.object(
        aggregation_tool,
        "_reaggregate_punctuality",
        return_value=pd.DataFrame(),
    )
    result = aggregation_tool.punctuality_by_geography()
    assert isinstance(result, pd.DataFrame)
