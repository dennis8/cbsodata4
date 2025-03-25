from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from cbsodata4.observations import get_observations


@patch("cbsodata4.observations.get_datasets")
@patch("cbsodata4.observations.download_dataset")
@patch("cbsodata4.observations.get_metadata")
@patch("cbsodata4.observations.pq.read_table")
@patch("cbsodata4.observations.Path.exists")
def test_get_observations_new_download(
    mock_exists,
    mock_read_table,
    mock_get_metadata,
    mock_download_dataset,
    mock_get_datasets,
):
    """Test retrieving observations with a new download."""
    # Mock dataset existence check
    mock_get_datasets.return_value = pd.DataFrame(
        {"Identifier": ["83133NED", "85039NED"], "Title": ["Dataset 1", "Dataset 2"]}
    )

    # Setup download path does not exist to trigger download
    mock_exists.side_effect = [
        False,
        True,
    ]  # First call returns False (to trigger download), second returns True

    # Mock metadata
    mock_meta = MagicMock()
    mock_download_dataset.return_value = mock_meta

    # Mock parquet reading
    mock_table = MagicMock()
    mock_df = pd.DataFrame({"Id": [1, 2], "Measure": ["M1", "M2"], "Value": [100, 200]})
    mock_table.to_pandas.return_value = mock_df
    mock_read_table.return_value = mock_table

    result = get_observations(id="83133NED")

    # Should trigger a download
    mock_download_dataset.assert_called_once()

    # Should read the observations from the downloaded files
    mock_read_table.assert_called_once()

    # Check that the result has the expected structure and metadata
    assert "Id" in result.columns
    assert "Measure" in result.columns
    assert "Value" in result.columns
    assert result.attrs["meta"] is mock_meta


@patch("cbsodata4.observations.get_datasets")
@patch("cbsodata4.observations.download_dataset")
@patch("cbsodata4.observations.get_metadata")
@patch("cbsodata4.observations.pq.read_table")
@patch("cbsodata4.observations.Path.exists")
def test_get_observations_existing_data(
    mock_exists,
    mock_read_table,
    mock_get_metadata,
    mock_download_dataset,
    mock_get_datasets,
):
    """Test retrieving observations using existing downloaded data."""
    # Mock dataset existence
    mock_get_datasets.return_value = pd.DataFrame(
        {"Identifier": ["83133NED"], "Title": ["Dataset 1"]}
    )

    # Setup download path exists to skip download
    mock_exists.return_value = True

    # Mock metadata
    mock_meta = MagicMock()
    mock_get_metadata.return_value = mock_meta

    # Mock parquet reading
    mock_table = MagicMock()
    mock_df = pd.DataFrame({"Id": [1, 2], "Measure": ["M1", "M2"], "Value": [100, 200]})
    mock_table.to_pandas.return_value = mock_df
    mock_read_table.return_value = mock_table

    result = get_observations(id="83133NED", overwrite=False)

    # Should not trigger a download
    mock_download_dataset.assert_not_called()

    # Should get metadata from API
    mock_get_metadata.assert_called_once()

    # Should read from existing data
    mock_read_table.assert_called_once()


@patch("cbsodata4.observations.get_datasets")
def test_get_observations_invalid_id(mock_get_datasets):
    """Test retrieving observations with an invalid dataset ID."""
    # Mock empty dataset list
    mock_get_datasets.return_value = pd.DataFrame(
        {"Identifier": ["83133NED"], "Title": ["Dataset 1"]}
    )

    # Call with non-existent ID
    with pytest.raises(ValueError, match="Table 'nonexistent' cannot be found"):
        get_observations(id="nonexistent")


@patch("cbsodata4.observations.get_datasets")
@patch("cbsodata4.observations.Path.exists")
def test_get_observations_missing_observations_dir(mock_exists, mock_get_datasets):
    """Test error when observations directory doesn't exist."""
    # Mock dataset existence
    mock_get_datasets.return_value = pd.DataFrame(
        {"Identifier": ["83133NED"], "Title": ["Dataset 1"]}
    )

    # First Path.exists check (for download directory) returns True
    # Second Path.exists check (for observations subdirectory) returns False
    mock_exists.side_effect = [True, False]

    with pytest.raises(FileNotFoundError, match="Observations directory not found"):
        get_observations(id="83133NED")


@patch("cbsodata4.observations.get_datasets")
@patch("cbsodata4.observations.download_dataset")
@patch("cbsodata4.observations.pq.read_table")
@patch("cbsodata4.observations.Path.exists")
def test_get_observations_include_id_flag(
    mock_exists, mock_read_table, mock_download_dataset, mock_get_datasets
):
    """Test controlling the inclusion of the Id column."""
    # Mock dataset existence
    mock_get_datasets.return_value = pd.DataFrame(
        {"Identifier": ["83133NED"], "Title": ["Dataset 1"]}
    )

    # Setup paths to exist
    mock_exists.return_value = True

    # Mock metadata and parquet reading
    mock_meta = MagicMock()
    mock_download_dataset.return_value = mock_meta

    mock_table = MagicMock()
    mock_df = pd.DataFrame({"Id": [1, 2], "Measure": ["M1", "M2"], "Value": [100, 200]})
    mock_table.to_pandas.return_value = mock_df
    mock_read_table.return_value = mock_table

    # Test with include_id=False
    result = get_observations(id="83133NED", include_id=False)
    assert "Id" not in result.columns

    # Test with include_id=True
    result = get_observations(id="83133NED", include_id=True)
    assert "Id" in result.columns
