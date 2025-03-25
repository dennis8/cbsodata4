from unittest.mock import patch

import pandas as pd

from cbsodata4.dataset_search import search_datasets


@patch("cbsodata4.dataset_search.fetch_json")
@patch("cbsodata4.dataset_search.get_datasets")
def test_search_datasets(mock_get_datasets, mock_fetch_json):
    """Test searching datasets with full results."""
    mock_fetch_json.return_value = {
        "results": [
            {
                "document_type": "table",
                "unique_id": "table1",
                "rel": 0.95,
                "url": "https://datasets.cbs.nl/table1",
            },
            {
                "document_type": "table",
                "unique_id": "table2",
                "rel": 0.85,
                "url": "https://datasets.cbs.nl/table2",
            },
            {
                "document_type": "publication",
                "unique_id": "pub1",
                "rel": 0.75,
                "url": "https://datasets.cbs.nl/pub1",
            },
        ]
    }

    mock_datasets = pd.DataFrame(
        {
            "Identifier": ["table1", "table2", "table3"],
            "Title": ["Table 1", "Table 2", "Table 3"],
            "Catalog": ["CBS", "CBS", "CBS"],
        }
    )
    mock_get_datasets.return_value = mock_datasets

    result = search_datasets("test query")

    mock_fetch_json.assert_called_once()
    call_url = mock_fetch_json.call_args[0][0]
    assert "query=test+query" in call_url

    assert len(result) == 2
    assert "rel" in result.columns
    assert "url" in result.columns
    assert result["Identifier"].tolist() == ["table1", "table2"]
    assert result["rel"].tolist() == [0.95, 0.85]


@patch("cbsodata4.dataset_search.fetch_json")
@patch("cbsodata4.dataset_search.get_datasets")
def test_search_datasets_no_results(mock_get_datasets, mock_fetch_json):
    """Test searching datasets with no matching results."""
    mock_fetch_json.return_value = {"results": []}

    mock_datasets = pd.DataFrame(
        {
            "Identifier": ["table1", "table2"],
            "Title": ["Table 1", "Table 2"],
            "Catalog": ["CBS", "CBS"],
        }
    )
    mock_get_datasets.return_value = mock_datasets

    result = search_datasets("nonexistent query")

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 0


@patch("cbsodata4.dataset_search.fetch_json")
@patch("cbsodata4.dataset_search.get_datasets")
def test_search_datasets_language_parameter(mock_get_datasets, mock_fetch_json):
    """Test searching datasets with custom language parameter."""
    mock_fetch_json.return_value = {"results": []}

    mock_datasets = pd.DataFrame(
        {
            "Identifier": ["table1", "table2"],
            "Title": ["Table 1", "Table 2"],
            "Catalog": ["CBS", "CBS"],
        }
    )
    mock_get_datasets.return_value = mock_datasets

    search_datasets("test query", language="en-gb")

    call_url = mock_fetch_json.call_args[0][0]
    assert "language=en-gb" in call_url
