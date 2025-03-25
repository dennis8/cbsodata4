from unittest.mock import patch

import pandas as pd

from cbsodata4.datasets import get_datasets


@patch("cbsodata4.datasets.fetch_json")
def test_get_datasets_with_date_conversion(mock_fetch_json):
    """Test retrieving datasets with date conversion."""
    mock_fetch_json.return_value = {
        "value": [
            {
                "Identifier": "table1",
                "Title": "Test Table 1",
                "Catalog": "CBS",
                "Modified": "2023-01-01T12:00:00Z",
                "ObservationsModified": "2023-01-02T12:00:00Z",
            },
            {
                "Identifier": "table2",
                "Title": "Test Table 2",
                "Catalog": "CBS",
                "Modified": "2023-02-01T12:00:00Z",
                "ObservationsModified": "2023-02-02T12:00:00Z",
            },
        ]
    }

    get_datasets.cache_clear()

    result = get_datasets(convert_dates=True)

    assert len(result) == 2
    assert "Identifier" in result.columns
    assert result["Identifier"].tolist() == ["table1", "table2"]

    assert pd.api.types.is_datetime64tz_dtype(result["Modified"].dtype)
    assert pd.api.types.is_datetime64tz_dtype(result["ObservationsModified"].dtype)

    assert result["Modified"].iloc[0].tzinfo is not None
    assert "Europe/Amsterdam" in str(result["Modified"].iloc[0].tzinfo)


@patch("cbsodata4.datasets.fetch_json")
def test_get_datasets_without_date_conversion(mock_fetch_json):
    """Test retrieving datasets without date conversion."""
    mock_fetch_json.return_value = {
        "value": [
            {
                "Identifier": "table1",
                "Title": "Test Table 1",
                "Catalog": "CBS",
                "Modified": "2023-01-01T12:00:00Z",
                "ObservationsModified": "2023-01-02T12:00:00Z",
            },
        ]
    }

    get_datasets.cache_clear()

    result = get_datasets(convert_dates=False)

    assert len(result) == 1
    assert result["Modified"].iloc[0] == "2023-01-01T12:00:00Z"
    assert not pd.api.types.is_datetime64_dtype(result["Modified"])


@patch("cbsodata4.datasets.fetch_json")
def test_get_datasets_catalog_filter(mock_fetch_json):
    """Test filtering datasets by catalog."""
    mock_fetch_json.return_value = {
        "value": [
            {"Identifier": "table1", "Catalog": "CBS"},
            {"Identifier": "table2", "Catalog": "OTHER"},
        ]
    }

    get_datasets.cache_clear()

    result = get_datasets(catalog="CBS", convert_dates=False)

    assert len(result) == 1
    assert result["Identifier"].iloc[0] == "table1"

    result_other = get_datasets(catalog="OTHER", convert_dates=False)
    assert len(result_other) == 1
    assert result_other["Identifier"].iloc[0] == "table2"


@patch("cbsodata4.datasets.fetch_json")
def test_get_datasets_caching(mock_fetch_json):
    """Test that get_datasets caches results."""
    get_datasets.cache_clear()

    mock_fetch_json.return_value = {
        "value": [{"Identifier": "table1", "Catalog": "CBS"}]
    }

    result1 = get_datasets(convert_dates=False)
    result2 = get_datasets(convert_dates=False)

    assert mock_fetch_json.call_count == 1
    pd.testing.assert_frame_equal(result1, result2)
