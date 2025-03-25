from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from cbsodata4.data_processor import get_wide_data


@patch("cbsodata4.data_processor.get_observations")
def test_get_wide_data_with_valid_data(mock_get_observations):
    """Test get_wide_data with valid input data."""
    mock_data = pd.DataFrame(
        {
            "Dim1": ["A", "A", "B", "B"],
            "Measure": ["M1", "M2", "M1", "M2"],
            "Value": [100, 200, 300, 400],
        }
    )

    mock_meta = MagicMock()
    mock_meta.dimension_identifiers = ["Dim1"]
    mock_meta.measurecode_mapping = {"M1": "Measure 1", "M2": "Measure 2"}
    mock_data.attrs["meta"] = mock_meta

    mock_get_observations.return_value = mock_data

    result = get_wide_data("test_id")

    mock_get_observations.assert_called_once()

    assert result.shape == (2, 3)
    assert set(result.columns) == {"Dim1", "Measure 1", "Measure 2"}

    assert result.loc[result["Dim1"] == "A", "Measure 1"].values[0] == 100
    assert result.loc[result["Dim1"] == "B", "Measure 2"].values[0] == 400


@patch("cbsodata4.data_processor.get_observations")
def test_get_wide_data_with_name_measure_false(mock_get_observations):
    """Test get_wide_data with name_measure_columns=False."""
    mock_data = pd.DataFrame(
        {"Dim1": ["A", "B"], "Measure": ["M1", "M1"], "Value": [100, 200]}
    )

    mock_meta = MagicMock()
    mock_meta.dimension_identifiers = ["Dim1"]
    mock_meta.measurecode_mapping = {"M1": "Measure 1"}
    mock_data.attrs["meta"] = mock_meta

    mock_get_observations.return_value = mock_data

    result = get_wide_data("test_id", name_measure_columns=False)

    assert "M1" in result.columns
    assert "Measure 1" not in result.columns


@patch("cbsodata4.data_processor.get_observations")
def test_get_wide_data_empty_results(mock_get_observations):
    """Test get_wide_data with empty results."""
    mock_data = pd.DataFrame(columns=["Dim1", "Measure", "Value"])
    mock_data = mock_data.astype(
        {"Dim1": "object", "Measure": "object", "Value": "float64"}
    )

    mock_meta = MagicMock()
    mock_meta.dimension_identifiers = ["Dim1"]
    mock_meta.measurecode_mapping = {"M1": "Measure 1", "M2": "Measure 2"}
    mock_data.attrs["meta"] = mock_meta

    mock_get_observations.return_value = mock_data

    result = get_wide_data("test_id")

    assert result.empty
    assert set(result.columns) == {"Measure 1", "Measure 2"}


@patch("cbsodata4.data_processor.get_observations")
def test_get_wide_data_error_no_meta(mock_get_observations):
    """Test get_wide_data error when metadata is missing."""
    mock_data = pd.DataFrame(
        {"Dim1": ["A", "B"], "Measure": ["M1", "M1"], "Value": [100, 200]}
    )

    mock_get_observations.return_value = mock_data

    with pytest.raises(ValueError, match="Metadata is missing"):
        get_wide_data("test_id")


@patch("cbsodata4.data_processor.get_observations")
def test_get_wide_data_error_no_dimensions(mock_get_observations):
    """Test get_wide_data error when dimensions are missing."""
    mock_data = pd.DataFrame({"Measure": ["M1", "M1"], "Value": [100, 200]})

    mock_meta = MagicMock()
    mock_meta.dimension_identifiers = []
    mock_data.attrs["meta"] = mock_meta

    mock_get_observations.return_value = mock_data

    with pytest.raises(ValueError, match="No dimensions found"):
        get_wide_data("test_id")
