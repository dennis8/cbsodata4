# date_handling.py

from typing import Any, Optional
import pandas as pd
from .metadata import CbsMeta
from datetime import datetime
from dateutil.relativedelta import relativedelta

def add_date_column(data: pd.DataFrame, 
                   date_type: str = "Date",
                   **kwargs) -> pd.DataFrame:
    """Convert the time variable into either a date or numeric.

    Add extra date columns to dataset for the creation of time series or graphics.

    Args:
        data (pd.DataFrame): DataFrame retrieved using get_data() or get_observations().
        date_type (str): Type of date column: "Date", "numeric".

    Returns:
        pd.DataFrame: Original dataset with two added columns: `<period>_Date` and `<period>_freq`.
    """
    meta: CbsMeta = data.attrs.get('meta')
    if meta is None:
        raise ValueError("add_date_column requires metadata.")

    # Find time dimension
    dimensions = meta.meta_dict.get('Dimensions', {}).get('Identifier', [])
    time_dimensions = [dim for dim in dimensions if dim == 'TimeDimension']  # Adjust based on actual metadata structure
    if not time_dimensions:
        print("Warning: No time dimension found!")
        return data

    period_col = time_dimensions[0]
    if period_col not in data.columns:
        print("Warning: Time dimension column not found in data.")
        return data

    period = data[period_col].astype(str)

    PATTERN = r"(\d{4})(\w{2})(\d{2})"

    data['year'] = period.str.extract(PATTERN, expand=False)[0].astype(int)
    data['type'] = period.str.extract(PATTERN, expand=False)[1]
    data['number'] = period.str.extract(PATTERN, expand=False)[2].astype(int)

    # Determine frequency
    data['is_year'] = data['type'] == "JJ"
    data['is_quarter'] = data['type'] == "KW"
    data['is_month'] = data['type'] == "MM"
    data['is_week'] = data['type'] == "W1"
    data['is_week_part'] = data['type'] == "X0"
    data['is_day'] = data['type'].str.contains(r"\d{2}")

    if date_type == "Date":
        data['period_Date'] = pd.NaT

        data.loc[data['is_year'], 'period_Date'] = pd.to_datetime(data['year'].astype(str) + "-01-01")
        data.loc[data['is_quarter'], 'period_Date'] = pd.to_datetime(data['year'].astype(str) + "-" + ((data['number'] -1)*3 +1).astype(str) + "-01")
        data.loc[data['is_month'], 'period_Date'] = pd.to_datetime(data['year'].astype(str) + "-" + data['number'].astype(str) + "-01")
        # For days, assuming type indicates the month?
        data.loc[data['is_day'], 'period_Date'] = pd.to_datetime(data['year'].astype(str) + "-" + data['type'].astype(str) + "-" + data['number'].astype(str), errors='coerce')
        # For weeks, approximate date as start of the week
        data.loc[data['is_week'], 'period_Date'] = data.apply(lambda row: get_week_start_date(row['year'], row['number']), axis=1)
        data.loc[data['is_week_part'], 'period_Date'] = pd.to_datetime(data['year'].astype(str) + "-01-01")

        data['period_Date'] = data['period_Date'].dt.date

    elif date_type == "numeric":
        data['period_numeric'] = 0.0
        data.loc[data['is_year'], 'period_numeric'] = data['year'] + 0.5
        data.loc[data['is_quarter'], 'period_numeric'] = data['year'] + (3*(data['number'] -1) +2)/12
        data.loc[data['is_month'], 'period_numeric'] = data['year'] + (data['number'] -0.5)/12
        data.loc[data['is_week'], 'period_numeric'] = data['year'] + (data['number'] -0.5)/53
        data.loc[data['is_week_part'], 'period_numeric'] = data['year']

        # If all frequencies are 'Y', make it integer
        if data['is_year'].all():
            data['period_numeric'] = data['period_numeric'].astype(int)

    # Determine frequency type
    freq_map = {
        'is_year': 'Y',
        'is_quarter': 'Q',
        'is_month': 'M',
        'is_day': 'D',
        'is_week': 'W',
        'is_week_part': 'X'
    }

    data['period_freq'] = 'Y'  # default
    for key, val in freq_map.items():
        data.loc[data[key], 'period_freq'] = val

    # Drop temporary columns
    data.drop(['year', 'type', 'number', 'is_year', 'is_quarter', 'is_month', 'is_week', 'is_week_part', 'is_day'], axis=1, inplace=True)

    # Insert the date columns after the period column
    cols = list(data.columns)
    period_idx = cols.index(period_col)
    if date_type == "Date":
        new_cols = ['period_Date', 'period_freq']
    elif date_type == "numeric":
        new_cols = ['period_numeric', 'period_freq']
    else:
        new_cols = []

    for new_col in reversed(new_cols):
        cols.insert(period_idx +1, cols.pop(cols.index(new_col)))

    data = data[cols]

    return data

def get_week_start_date(year: int, week: int) -> Optional[datetime.date]:
    """Get the start date of a week given year and week number.

    Args:
        year (int): Year.
        week (int): ISO week number.

    Returns:
        Optional[datetime.date]: Start date of the week.
    """
    try:
        return datetime.strptime(f'{year}-W{week}-1', "%G-W%V-%u").date()
    except ValueError:
        return None
