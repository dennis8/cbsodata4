from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest

from cbsodata4.unit_handler import add_unit_column


def test_add_unit_column_success():
    data = pd.DataFrame({"Measure": ["M1", "M2"], "Value": [100, 200]})
    mock_meta = MagicMock()
    mock_meta.meta_dict = {
        "MeasureCodes": [{"Identifier": "M1", "Unit": "Unit1"}, {"Identifier": "M2", "Unit": "Unit2"}]
    }
    data.attrs["meta"] = mock_meta

    df = add_unit_column(data)
    assert "Unit" in df.columns
    assert df["Unit"].tolist() == ["Unit1", "Unit2"]
    # Check column order: 'Unit' after 'Value'
    value_idx = df.columns.get_loc("Value")
    unit_idx = df.columns.get_loc("Unit")
    assert unit_idx == value_idx + 1


def test_add_unit_column_no_meta():
    data = pd.DataFrame({"Measure": ["M1", "M2"], "Value": [100, 200]})
    # No meta
    with pytest.raises(ValueError, match="add_unit_column requires metadata."):
        add_unit_column(data)


def test_add_unit_column_no_measure_column():
    data = pd.DataFrame({"MeasureCode": ["M1", "M2"], "Value": [100, 200]})
    mock_meta = MagicMock()
    mock_meta.meta_dict = {
        "MeasureCodes": [{"Identifier": "M1", "Unit": "Unit1"}, {"Identifier": "M2", "Unit": "Unit2"}]
    }
    data.attrs["meta"] = mock_meta

    with pytest.raises(ValueError, match="Data does not contain 'Measure' column."):
        add_unit_column(data)


def test_add_unit_column_partial_mapping():
    data = pd.DataFrame(
        {
            "Measure": ["M1", "M3"],  # M3 not in mapping
            "Value": [100, 200],
        }
    )
    mock_meta = MagicMock()
    mock_meta.meta_dict = {
        "MeasureCodes": [{"Identifier": "M1", "Unit": "Unit1"}, {"Identifier": "M2", "Unit": "Unit2"}]
    }
    data.attrs["meta"] = mock_meta

    df = add_unit_column(data)
    assert "Unit" in df.columns
    assert df["Unit"].tolist() == ["Unit1", np.nan]
