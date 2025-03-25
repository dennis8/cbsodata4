from unittest.mock import MagicMock, mock_open, patch

import pandas as pd
import pytest

import cbsodata4


@pytest.fixture
def mock_dataset_responses():
    """Mock responses for dataset-related API calls."""
    datasets_response = {
        "value": [
            {
                "Identifier": "test_id",
                "Title": "Test Dataset",
                "Catalog": "CBS",
                "Modified": "2023-01-01T12:00:00Z",
            }
        ]
    }

    dataset_metadata = {"value": [{"name": "Dimensions"}, {"name": "MeasureCodes"}]}

    dimensions = {
        "value": [
            {"Identifier": "Dim1", "Kind": "Dimension"},
            {"Identifier": "Period", "Kind": "TimeDimension"},
        ]
    }

    measure_codes = {
        "value": [
            {"Identifier": "M1", "Title": "Measure 1", "Unit": "Unit1"},
            {"Identifier": "M2", "Title": "Measure 2", "Unit": "Unit2"},
        ]
    }

    dim1_codes = {
        "value": [
            {"Identifier": "D1", "Title": "Dimension 1"},
            {"Identifier": "D2", "Title": "Dimension 2"},
        ]
    }

    properties = {
        "Identifier": "test_id",
        "Title": "Test Dataset",
        "Description": "This is a test dataset",
    }

    observations = {
        "value": [
            {
                "Id": 1,
                "Measure": "M1",
                "Dim1": "D1",
                "Period": "2023MM01",
                "Value": 100,
            },
            {
                "Id": 2,
                "Measure": "M2",
                "Dim1": "D1",
                "Period": "2023MM01",
                "Value": 200,
            },
            {
                "Id": 3,
                "Measure": "M1",
                "Dim1": "D2",
                "Period": "2023MM01",
                "Value": 300,
            },
            {
                "Id": 4,
                "Measure": "M2",
                "Dim1": "D2",
                "Period": "2023MM01",
                "Value": 400,
            },
        ]
    }

    return {
        "datasets": datasets_response,
        "dataset_metadata": dataset_metadata,
        "dimensions": dimensions,
        "measure_codes": measure_codes,
        "dim1_codes": dim1_codes,
        "properties": properties,
        "observations": observations,
    }


@pytest.fixture
def setup_temp_dir(tmp_path):
    """Set up a temporary directory for testing."""
    test_dir = tmp_path / "test_data"
    test_dir.mkdir()
    return test_dir


def mock_json_response(url, responses):
    """Return appropriate mock response based on URL pattern."""
    if "/Observations" in url:
        return responses["observations"]
    elif "/Properties" in url:
        return responses["properties"]
    elif "/Dimensions" in url:
        return responses["dimensions"]
    elif "/MeasureCodes" in url:
        return responses["measure_codes"]
    elif "/Dim1Codes" in url:
        return responses["dim1_codes"]
    elif url.endswith("/test_id"):
        return responses["dataset_metadata"]
    else:
        return {"value": []}


@patch("httpx.get")
@patch("cbsodata4.observations.Path.exists")
@patch("cbsodata4.observations.get_datasets")
def test_integration_get_observations(
    mock_get_datasets,
    mock_path_exists,
    mock_get,
    mock_dataset_responses,
    setup_temp_dir,
):
    """Test the complete flow of getting observations."""
    responses = mock_dataset_responses

    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_response.json.side_effect = lambda: mock_json_response(
        mock_get.call_args[0][0], responses
    )
    mock_get.return_value = mock_response

    mock_get_datasets.return_value = pd.DataFrame(
        {"Identifier": ["test_id"], "Title": ["Test Dataset"], "Catalog": ["CBS"]}
    )

    mock_path_exists.return_value = True

    with (
        patch("pathlib.Path.mkdir", return_value=None),
        patch("pandas.DataFrame.to_parquet", return_value=None),
        patch("builtins.open", mock_open()),
        patch("json.dump", return_value=None),
        patch("pyarrow.parquet.read_table") as mock_read_table,
    ):
        mock_table = pd.DataFrame(responses["observations"]["value"])
        mock_read_table.return_value.to_pandas.return_value = mock_table

        result = cbsodata4.get_observations(
            id="test_id", download_dir=setup_temp_dir, overwrite=True
        )

        assert not result.empty
        assert "Measure" in result.columns
        assert "Dim1" in result.columns
        assert "Period" in result.columns
        assert "Value" in result.columns
        assert len(result) == 4
        assert "meta" in result.attrs

        result_with_labels = cbsodata4.add_label_columns(result)
        assert "MeasureLabel" in result_with_labels.columns
        assert "Dim1Label" in result_with_labels.columns

        result_with_units = cbsodata4.add_unit_column(result)
        assert "Unit" in result_with_units.columns

        result_with_dates = cbsodata4.add_date_column(result)
        assert "Period_date" in result_with_dates.columns
        assert "Period_freq" in result_with_dates.columns

        meta = result.attrs["meta"]
        assert meta.dimension_identifiers == ["Dim1", "Period"]
        assert meta.time_dimension_identifiers == ["Period"]


@patch("httpx.get")
@patch("cbsodata4.observations.Path.exists")
@patch("cbsodata4.observations.get_datasets")
def test_integration_get_wide_data(
    mock_get_datasets,
    mock_path_exists,
    mock_get,
    mock_dataset_responses,
    setup_temp_dir,
):
    """Test the complete flow of getting wide data format."""
    responses = mock_dataset_responses

    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_response.json.side_effect = lambda: mock_json_response(
        mock_get.call_args[0][0], responses
    )
    mock_get.return_value = mock_response

    mock_get_datasets.return_value = pd.DataFrame(
        {"Identifier": ["test_id"], "Title": ["Test Dataset"], "Catalog": ["CBS"]}
    )

    mock_path_exists.return_value = True

    with (
        patch("pathlib.Path.mkdir", return_value=None),
        patch("pandas.DataFrame.to_parquet", return_value=None),
        patch("builtins.open", mock_open()),
        patch("json.dump", return_value=None),
        patch("pyarrow.parquet.read_table") as mock_read_table,
    ):
        mock_table = pd.DataFrame(responses["observations"]["value"])
        mock_read_table.return_value.to_pandas.return_value = mock_table

        result = cbsodata4.get_wide_data(
            id="test_id", download_dir=setup_temp_dir, overwrite=True
        )

        assert not result.empty
        assert "Dim1" in result.columns
        assert "Period" in result.columns
        assert "Measure 1" in result.columns
        assert "Measure 2" in result.columns
        assert len(result) == 2

        d1_row = result[result["Dim1"] == "D1"]
        assert d1_row["Measure 1"].values[0] == 100
        assert d1_row["Measure 2"].values[0] == 200


@patch("cbsodata4.dataset_search.fetch_json")
@patch("cbsodata4.dataset_search.get_datasets")
def test_integration_dataset_search(
    mock_get_datasets, mock_fetch_json, mock_dataset_responses
):
    """Test the dataset search functionality."""
    search_response = {
        "results": [
            {
                "document_type": "table",
                "unique_id": "test_id",
                "rel": 0.95,
                "url": "https://datasets.cbs.nl/test_id",
            }
        ]
    }

    mock_get_datasets.return_value = pd.DataFrame(
        {"Identifier": ["test_id"], "Title": ["Test Dataset"], "Catalog": ["CBS"]}
    )

    mock_fetch_json.return_value = search_response

    result = cbsodata4.search_datasets("test query")

    assert isinstance(result, pd.DataFrame)
    assert "Identifier" in result.columns
    assert "rel" in result.columns
    assert "url" in result.columns
    assert len(result) > 0
    assert result["Identifier"].iloc[0] == "test_id"
    assert result["rel"].iloc[0] == 0.95
