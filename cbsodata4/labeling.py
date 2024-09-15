# labeling.py

from typing import Any
import pandas as pd
from .metadata import CbsMeta

def add_label_columns(data: pd.DataFrame, **kwargs) -> pd.DataFrame:
    """Add understandable labels to a table.

    Adds for the `Measure` and each Dimension column an extra column `MeasureLabel` (`<Dimension>Label`) that contains the `Title` of each code.

    Args:
        data (pd.DataFrame): DataFrame retrieved using get_data() or get_observations().

    Returns:
        pd.DataFrame: Original DataFrame with extra label columns.
    """
    meta: CbsMeta = data.attrs.get('meta')
    if meta is None:
        raise ValueError("add_label_columns only works on data retrieved with get_data or get_observations and requires metadata.")

    # Identify dimension columns
    dimension_identifiers = meta.meta_dict.get('Dimensions', {}).get('Identifier', [])
    dim_cols = ['Measure'] + dimension_identifiers
    dim_cols = [col for col in dim_cols if col in data.columns]

    dim_codes = [f"{col}Codes" for col in dim_cols]

    label_cols = [f"{col}Label" for col in dim_cols]

    for dim_col, code_col, label_col in zip(dim_cols, dim_codes, label_cols):
        codes = data[dim_col]
        if hasattr(meta, code_col):
            meta_dim = getattr(meta, code_col)
            # meta_dim is a list of dicts with 'Identifier' and 'Title'
            code_to_title = {item['Identifier']: item['Title'] for item in meta_dim}
            data[label_col] = codes.map(code_to_title)
        else:
            data[label_col] = None  # Or leave it out, or raise an error

    # Reorder columns: place label columns just after the code columns
    columns = list(data.columns)
    for dim_col, label_col in zip(dim_cols, label_cols):
        if dim_col in columns and label_col in columns:
            idx = columns.index(dim_col)
            columns.insert(idx +1, columns.pop(columns.index(label_col)))
    data = data[columns]

    return data
