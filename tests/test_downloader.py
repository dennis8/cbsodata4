from pathlib import Path
from unittest.mock import MagicMock, mock_open, patch

import pandas as pd

from cbsodata4.downloader import (
    download_data_stream,
    download_dataset,
    get_empty_dataframe,
)


@patch("cbsodata4.downloader.get_metadata")
@patch("cbsodata4.downloader.download_data_stream")
@patch("cbsodata4.downloader.Path.mkdir")
@patch("builtins.open", new_callable=mock_open)
@patch("cbsodata4.downloader.pd.DataFrame.to_parquet")
def test_download_dataset(
    mock_to_parquet, mock_file_open, mock_mkdir, mock_download_data, mock_get_metadata
):
    """Test downloading a dataset."""
    mock_meta = MagicMock()
    mock_meta.meta_dict = {
        "Dimensions": [{"Identifier": "Dim1"}],
        "MeasureCodes": [{"Identifier": "M1", "Title": "Measure 1"}],
        "Properties": {"Identifier": "test_id", "Title": "Test Dataset"},
    }
    mock_get_metadata.return_value = mock_meta

    result = download_dataset("test_id")

    mock_mkdir.assert_called()

    assert mock_to_parquet.call_count >= 2
    assert mock_file_open.call_count >= 1

    mock_download_data.assert_called_once()

    assert result is mock_meta


@patch("cbsodata4.downloader.get_metadata")
@patch("cbsodata4.downloader.download_data_stream")
@patch("cbsodata4.downloader.Path.mkdir")
@patch("builtins.open", new_callable=mock_open)
@patch("cbsodata4.downloader.pd.DataFrame.to_parquet")
def test_download_dataset_with_filters(
    mock_to_parquet, mock_file_open, mock_mkdir, mock_download_data, mock_get_metadata
):
    """Test downloading a dataset with filters."""
    mock_meta = MagicMock()
    mock_meta.meta_dict = {"Dimensions": [], "Properties": {}}
    mock_get_metadata.return_value = mock_meta

    result = download_dataset("test_id", Dim1="Value1", Dim2=["Value2", "Value3"])

    call_url = mock_download_data.call_args[1]["url"]
    assert "filter=" in call_url
    assert "Dim1" in call_url
    assert "Dim2" in call_url

    download_dataset("test_id", select=["Field1", "Field2"])
    call_url = mock_download_data.call_args[1]["url"]
    assert "$select=Field1,Field2" in call_url


@patch("cbsodata4.downloader.fetch_json")
@patch("cbsodata4.downloader.pd.DataFrame.to_parquet")
@patch("cbsodata4.downloader.Path.mkdir")
def test_download_data_stream(mock_mkdir, mock_to_parquet, mock_fetch_json):
    """Test downloading data stream."""
    mock_fetch_json.side_effect = [
        {"value": [{"Id": 1, "Value": 100}], "@odata.nextLink": "https://next.page"},
        {"value": [{"Id": 2, "Value": 200}], "@odata.nextLink": None},
    ]

    download_data_stream(
        url="https://test.url",
        output_path=str(Path("test_output")),
        empty_selection=pd.DataFrame(),
    )

    assert mock_fetch_json.call_count == 2

    assert mock_to_parquet.call_count == 2
    assert "partition_0" in mock_to_parquet.call_args_list[0][0][0]
    assert "partition_1" in mock_to_parquet.call_args_list[1][0][0]


def test_get_empty_dataframe():
    """Test creating an empty dataframe with the right structure."""
    mock_meta = MagicMock()
    mock_meta.dimension_identifiers = ["Dim1", "Dim2"]

    df = get_empty_dataframe(mock_meta)

    expected_columns = ["Id", "Measure", "ValueAttribute", "Value", "Dim1", "Dim2"]
    assert all(col in df.columns for col in expected_columns)
    assert len(df) == 0
