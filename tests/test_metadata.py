from unittest.mock import patch

import pandas as pd

from cbsodata4.metadata import CbsMetadata, get_metadata


def test_cbs_metadata_properties():
    """Test the properties of the CbsMetadata class."""
    meta_dict = {
        "Properties": {"Identifier": "test_id", "Title": "Test Dataset"},
        "Dimensions": [
            {"Identifier": "Dim1", "Kind": "Dimension"},
            {"Identifier": "Period", "Kind": "TimeDimension"},
        ],
        "MeasureCodes": [
            {"Identifier": "M1", "Title": "Measure 1"},
            {"Identifier": "M2", "Title": "Measure 2"},
        ],
        "Dim1Codes": [
            {"Identifier": "D1", "Title": "Dimension 1"},
            {"Identifier": "D2", "Title": "Dimension 2"},
        ],
    }
    meta = CbsMetadata(meta_dict)

    assert meta.identifier == "test_id"
    assert meta.title == "Test Dataset"
    assert meta.dimension_identifiers == ["Dim1", "Period"]
    assert meta.time_dimension_identifiers == ["Period"]
    assert "MeasureCodes" in meta.get_codes()
    assert "Dim1Codes" in meta.get_codes()


def test_cbs_metadata_mappings():
    """Test the mapping methods of the CbsMetadata class."""
    meta_dict = {
        "MeasureCodes": [
            {"Identifier": "M1", "Title": "Measure 1"},
            {"Identifier": "M2", "Title": "Measure 2"},
        ],
        "Dim1Codes": [
            {"Identifier": "D1", "Title": "Dimension 1"},
            {"Identifier": "D2", "Title": "Dimension 2"},
        ],
        "Dimensions": [{"Identifier": "Dim1", "Kind": "Dimension"}],
    }
    meta = CbsMetadata(meta_dict)

    assert meta.measurecode_mapping == {"M1": "Measure 1", "M2": "Measure 2"}
    assert meta.get_dimension_mapping("Dim1") == {
        "D1": "Dimension 1",
        "D2": "Dimension 2",
    }

    label_mappings = meta.get_label_mappings()
    assert "Measure" in label_mappings
    assert "Dim1" in label_mappings
    assert label_mappings["Measure"] == {"M1": "Measure 1", "M2": "Measure 2"}
    assert meta.get_label_columns() == ["MeasureLabel", "Dim1Label"]


def test_cbs_metadata_representation():
    """Test the string representation of the CbsMetadata class."""
    meta_dict = {
        "Properties": {"Identifier": "test_id", "Title": "Test Dataset"},
        "Dimensions": [{"Identifier": "Dim1", "Kind": "Dimension"}],
    }
    meta = CbsMetadata(meta_dict)

    repr_str = repr(meta)
    assert "test_id" in repr_str
    assert "Test Dataset" in repr_str
    assert "Dim1" in repr_str


@patch("cbsodata4.metadata.fetch_json")
def test_get_metadata_from_id(mock_fetch_json):
    """Test retrieving metadata for a dataset ID."""
    # Mock the API responses
    mock_fetch_json.side_effect = [
        {"value": [{"name": "Dimensions"}, {"name": "MeasureCodes"}]},  # main endpoint
        {"value": [{"Identifier": "Dim1"}]},  # Dimensions
        {"value": [{"Identifier": "M1", "Title": "Measure 1"}]},  # MeasureCodes
        {"Identifier": "test_id", "Title": "Test Dataset"},  # Properties
    ]

    meta = get_metadata("test_id")

    assert isinstance(meta, CbsMetadata)
    assert meta.meta_dict["Dimensions"] == [{"Identifier": "Dim1"}]
    assert meta.meta_dict["MeasureCodes"] == [
        {"Identifier": "M1", "Title": "Measure 1"}
    ]
    assert meta.meta_dict["Properties"] == {
        "Identifier": "test_id",
        "Title": "Test Dataset",
    }

    # Verify the API calls
    assert mock_fetch_json.call_count == 4


def test_get_metadata_from_dataframe():
    """Test retrieving metadata from a DataFrame with metadata attribute."""
    df = pd.DataFrame({"test": [1, 2, 3]})
    meta = CbsMetadata({"Properties": {"Identifier": "test_id"}})
    df.attrs["meta"] = meta

    # Test getting metadata from DataFrame
    result = get_metadata(df)
    assert result is meta
