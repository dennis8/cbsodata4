from datetime import datetime
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from cbsodata4.date_handler import add_date_column, period_to_date, period_to_freq, period_to_numeric


def test_period_to_date_jj():
    period = "2023JJ01"
    expected = datetime(2023, 1, 1)
    assert period_to_date(period) == expected


def test_period_to_date_kw():
    period = "2023KW02"  # Second quarter
    expected = datetime(2023, 4, 1)
    assert period_to_date(period) == expected


def test_period_to_date_mm():
    period = "2023MM05"  # May
    expected = datetime(2023, 5, 1)
    assert period_to_date(period) == expected


def test_period_to_date_w1():
    period = "2023W101"  # 1st week
    expected = datetime(2023, 1, 1) + pd.Timedelta(weeks=0)
    assert period_to_date(period) == expected


def test_period_to_date_x0():
    period = "2023X000"
    expected = datetime(2023, 1, 1)
    assert period_to_date(period) == expected


def test_period_to_date_unknown():
    period = "2023ZZ99"
    with pytest.raises(ValueError):
        period_to_date(period)


def test_period_to_numeric():
    period = "2023MM05"
    date = period_to_date(period)
    expected = date.year + (date.timetuple().tm_yday - 1) / 365
    assert period_to_numeric(period) == expected


def test_period_to_freq():
    assert period_to_freq("2023JJ01") == "Y"
    assert period_to_freq("2023KW02") == "Q"
    assert period_to_freq("2023MM05") == "M"
    assert period_to_freq("2023W101") == "W"
    assert period_to_freq("2023X000") == "X"
    assert period_to_freq("2023ZZ99") == "D"  # default


def test_add_date_column_date():
    data = pd.DataFrame({"Dim1": ["D1", "D2"], "Period": ["2023MM05", "2023MM06"]})
    meta = MagicMock()
    meta.time_dimension_identifiers = ["Period"]
    data.attrs["meta"] = meta

    df = add_date_column(data, date_type="date")
    assert "Period_date" in df.columns
    assert "Period_freq" in df.columns
    assert df["Period_date"].iloc[0] == datetime(2023, 5, 1)
    assert df["Period_freq"].dtype.name == "category"


def test_add_date_column_numeric():
    data = pd.DataFrame({"Dim1": ["D1", "D2"], "Period": ["2023MM05", "2023MM06"]})
    meta = MagicMock()
    meta.time_dimension_identifiers = ["Period"]
    data.attrs["meta"] = meta

    df = add_date_column(data, date_type="numeric")
    assert "Period_numeric" in df.columns
    assert "Period_freq" in df.columns
    assert df["Period_numeric"].iloc[0] == pytest.approx(2023.329, rel=1e-3)
    assert df["Period_freq"].dtype.name == "category"


def test_add_date_column_no_meta():
    data = pd.DataFrame({"Dim1": ["D1"], "Period": ["2023MM05"]})
    # No meta
    with pytest.raises(ValueError, match="add_date_column requires metadata."):
        add_date_column(data)


def test_add_date_column_no_time_dimensions():
    data = pd.DataFrame({"Dim1": ["D1"], "Period": ["2023MM05"]})
    meta = MagicMock()
    meta.time_dimension_identifiers = []
    data.attrs["meta"] = meta

    with patch("cbsodata4.date_handler.logger.warning") as mock_logger:
        df = add_date_column(data)
        mock_logger.assert_called_with("Time dimension column not found in data.")
        # Data should be unchanged
        pd.testing.assert_frame_equal(df, data)
