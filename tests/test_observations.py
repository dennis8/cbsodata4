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
    mock_get_datasets.return_value = pd.DataFrame(
        {"Identifier": ["83133NED", "85039NED"], "Title": ["Dataset 1", "Dataset 2"]}
    )

    mock_exists.side_effect = [
        False,
        True,
    ]

    mock_meta = MagicMock()
    mock_download_dataset.return_value = mock_meta

    mock_table = MagicMock()
    mock_df = pd.DataFrame({"Id": [1, 2], "Measure": ["M1", "M2"], "Value": [100, 200]})
    mock_table.to_pandas.return_value = mock_df
    mock_read_table.return_value = mock_table

    result = get_observations(id="83133NED")

    mock_download_dataset.assert_called_once()

    mock_read_table.assert_called_once()

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
    mock_get_datasets.return_value = pd.DataFrame(
        {"Identifier": ["83133NED"], "Title": ["Dataset 1"]}
    )

    mock_exists.return_value = True

    mock_meta = MagicMock()
    mock_get_metadata.return_value = mock_meta

    mock_table = MagicMock()
    mock_df = pd.DataFrame({"Id": [1, 2], "Measure": ["M1", "M2"], "Value": [100, 200]})
    mock_table.to_pandas.return_value = mock_df
    mock_read_table.return_value = mock_table

    result = get_observations(id="83133NED", overwrite=False)

    mock_download_dataset.assert_not_called()

    mock_get_metadata.assert_called_once()

    mock_read_table.assert_called_once()


@patch("cbsodata4.observations.get_datasets")
def test_get_observations_invalid_id(mock_get_datasets):
    """Test retrieving observations with an invalid dataset ID."""
    mock_get_datasets.return_value = pd.DataFrame(
        {"Identifier": ["83133NED"], "Title": ["Dataset 1"]}
    )

    with pytest.raises(ValueError, match="Table 'nonexistent' cannot be found"):
        get_observations(id="nonexistent")


@patch("cbsodata4.observations.get_datasets")
@patch("cbsodata4.observations.Path.exists")
def test_get_observations_missing_observations_dir(mock_exists, mock_get_datasets):
    """Test error when observations directory doesn't exist."""
    mock_get_datasets.return_value = pd.DataFrame(
        {"Identifier": ["83133NED"], "Title": ["Dataset 1"]}
    )

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
    mock_get_datasets.return_value = pd.DataFrame(
        {"Identifier": ["83133NED"], "Title": ["Dataset 1"]}
    )

    mock_exists.return_value = True

    mock_meta = MagicMock()
    mock_download_dataset.return_value = mock_meta

    mock_table = MagicMock()
    mock_df = pd.DataFrame({"Id": [1, 2], "Measure": ["M1", "M2"], "Value": [100, 200]})
    mock_table.to_pandas.return_value = mock_df
    mock_read_table.return_value = mock_table

    result = get_observations(id="83133NED", include_id=False)
    assert "Id" not in result.columns

    result = get_observations(id="83133NED", include_id=True)
    assert "Id" in result.columns
