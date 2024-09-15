import logging
from datetime import datetime
from functools import cache
from typing import Literal

import pandas as pd

from .metadata import CbsMetadata

logger = logging.getLogger(__name__)


@cache
def period_to_date(period: str) -> datetime:
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
    date = period_to_date(period)
    return date.year + (date.timetuple().tm_yday - 1) / (366 if date.is_leap_year else 365)


@cache
def period_to_freq(period: str) -> str:
    type_ = period[4:6]
    freq_map = {"JJ": "Y", "KW": "Q", "MM": "M", "W1": "W", "X0": "X"}
    return freq_map.get(type_, "D")


def add_date_column(data: pd.DataFrame, date_type: Literal["date", "numeric"] = "date"):
    meta: CbsMetadata = data.attrs.get("meta")
    if meta is None:
        logger.error("add_date_column requires metadata.")
        raise ValueError("add_date_column requires metadata.")

    time_dimensions = meta.time_dimension_identifiers

    if not time_dimensions:
        logger.warning("Time dimension column not found in data.")
        return data

    for period_name in time_dimensions:
        periods = data[period_name]

        if date_type == "date":
            new_column = periods.map(period_to_date)
        elif date_type == "numeric":
            new_column = periods.map(period_to_numeric)
        else:
            raise ValueError("date_type must be either 'date' or 'numeric'")

        freq_column = periods.map(period_to_freq)
        freq_column = pd.Categorical(freq_column, categories=["Y", "Q", "M", "D", "W", "X"])

        insert_loc = data.columns.get_loc(period_name) + 1
        data.insert(insert_loc, f"{period_name}_{date_type}", new_column)
        data.insert(insert_loc + 1, f"{period_name}_freq", freq_column)

    return data
