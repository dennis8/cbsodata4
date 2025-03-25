from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest

from cbsodata4.labeler import add_label_columns


def test_add_label_columns_success():
    data = pd.DataFrame({"Measure": ["M1", "M2"], "Dim1": ["D1", "D2"]})
    mock_meta = MagicMock()
    mock_meta.get_label_mappings.return_value = {
        "Measure": {"M1": "Measure 1", "M2": "Measure 2"},
        "Dim1": {"D1": "Dimension 1", "D2": "Dimension 2"},
    }
    mock_meta.dimension_identifiers = ["Dim1"]
    data.attrs["meta"] = mock_meta

    df = add_label_columns(data)
    assert "MeasureLabel" in df.columns
    assert "Dim1Label" in df.columns
    assert df["MeasureLabel"].tolist() == ["Measure 1", "Measure 2"]
    assert df["Dim1Label"].tolist() == ["Dimension 1", "Dimension 2"]
    cols = list(df.columns)
    assert cols == ["Measure", "MeasureLabel", "Dim1", "Dim1Label"]


def test_add_label_columns_no_meta():
    data = pd.DataFrame({"Measure": ["M1", "M2"], "Dim1": ["D1", "D2"]})
    with pytest.raises(ValueError, match="add_label_columns requires metadata."):
        add_label_columns(data)


def test_add_label_columns_no_measure_column():
    data = pd.DataFrame({"MeasureCode": ["M1", "M2"], "Dim2": ["D1", "D2"]})
    mock_meta = MagicMock()
    mock_meta.get_label_mappings.return_value = {
        "Measure": {"M1": "Measure 1", "M2": "Measure 2"},
        "Dim1": {"D1": "Dimension 1", "D2": "Dimension 2"},
    }
    data.attrs["meta"] = mock_meta

    with pytest.raises(
        ValueError, match="Data does not contain column 'Dim1' required for labeling."
    ):
        add_label_columns(data)


def test_add_label_columns_partial_mappings():
    data = pd.DataFrame({"Measure": ["M1", "M3"], "Dim1": ["D1", "D3"]})
    mock_meta = MagicMock()
    mock_meta.get_label_mappings.return_value = {
        "Measure": {"M1": "Measure 1"},
        "Dim1": {"D1": "Dimension 1"},
    }
    mock_meta.dimension_identifiers = ["Dim1"]
    data.attrs["meta"] = mock_meta

    df = add_label_columns(data)
    assert "MeasureLabel" in df.columns
    assert "Dim1Label" in df.columns
    assert df["MeasureLabel"].tolist() == ["Measure 1", np.nan]
    assert df["Dim1Label"].tolist() == ["Dimension 1", np.nan]


def test_add_label_columns_column_ordering():
    data = pd.DataFrame({"Measure": ["M1"], "Dim1": ["D1"], "Other": ["O1"]})
    mock_meta = MagicMock()
    mock_meta.get_label_mappings.return_value = {
        "Measure": {"M1": "Measure 1"},
        "Dim1": {"D1": "Dimension 1"},
    }
    mock_meta.dimension_identifiers = ["Dim1"]
    data.attrs["meta"] = mock_meta

    df = add_label_columns(data)
    cols = list(df.columns)
    assert cols == ["Measure", "MeasureLabel", "Dim1", "Dim1Label", "Other"]
