import calendar
import logging
from datetime import datetime
from functools import cache
from typing import Literal

import pandas as pd

from .metadata import CbsMetadata

logger = logging.getLogger(__name__)


@cache
def period_to_date(period: str) -> datetime:
    """Convert a CBS period string to a datetime object."""
    year, type_, number = int(period[:4]), period[4:6], int(period[6:])
    base_date = datetime(year, 1, 1)

    if type_ == "JJ":
        return base_date
    elif type_ == "KW":
        return base_date.replace(month=1 + 3 * (number - 1))
    elif type_ == "MM":
        return base_date.replace(month=number)
    elif type_ == "W1":
        return base_date + pd.Timedelta(weeks=number - 1)
    elif type_ == "X0":
        return base_date
    else:
        return base_date.replace(month=int(type_), day=number)


@cache
def period_to_numeric(period: str) -> float:
    """Convert a CBS period string to a numeric representation."""
    date = period_to_date(period)
    days_in_year = 366 if calendar.isleap(date.year) else 365
    return date.year + (date.timetuple().tm_yday - 1) / days_in_year


@cache
def period_to_freq(period: str) -> str:
    """Convert a CBS period string to a frequency indicator."""
    return {"JJ": "Y", "KW": "Q", "MM": "M", "W1": "W", "X0": "X"}.get(period[4:6], "D")


def add_date_column(
    data: pd.DataFrame, date_type: Literal["date", "numeric"] = "date"
) -> pd.DataFrame:
    """Add date columns for time dimensions in the data using the specified date type."""
    meta: CbsMetadata = data.attrs.get("meta")
    if meta is None:
        raise ValueError("add_date_column requires metadata.")

    time_dimensions = meta.time_dimension_identifiers
    if not time_dimensions:
        logger.warning("Time dimension column not found in data.")
        return data

    new_columns = {}
    for period_name in time_dimensions:
        periods = data[period_name]

        converter = period_to_date if date_type == "date" else period_to_numeric
        new_columns[f"{period_name}_{date_type}"] = periods.map(converter)

        freq_column = periods.map(period_to_freq)
        new_columns[f"{period_name}_freq"] = pd.Categorical(
            freq_column, categories=["Y", "Q", "M", "D", "W", "X"]
        )

    result = data.assign(**new_columns)

    cols = []
    for col in data.columns:
        cols.append(col)
        if col in time_dimensions:
            cols.append(f"{col}_{date_type}")
            cols.append(f"{col}_freq")

    result = result[cols]
    result.attrs = data.attrs.copy()
    return result
