from typing import Any
import pandas as pd
from .metadata import CbsMetadata
import logging

logger = logging.getLogger(__name__)

def add_label_columns(data: pd.DataFrame) -> pd.DataFrame:
    """Add descriptive label columns based on metadata.

    For each Measure and Dimension column, adds a corresponding Label column containing the Title from metadata.

    Args:
        data (pd.DataFrame): DataFrame retrieved using get_wide_data() or get_observations().

    Returns:
        pd.DataFrame: Original DataFrame with additional label columns.
    """
    meta: CbsMetadata = data.attrs.get('meta')
    if meta is None:
        logger.error("add_label_columns requires metadata.")
        raise ValueError("add_label_columns requires metadata.")
    
    # Identify dimension columns
    dimension_identifiers = [dim['Identifier'] for dim in meta.meta_dict.get('Dimensions', [])]
    dim_cols = ['Measure'] + dimension_identifiers
    dim_cols = [col for col in dim_cols if col in data.columns]
    
    label_cols = [f"{col}Label" for col in dim_cols]
    
    for dim_col, label_col in zip(dim_cols, label_cols):
        if dim_col == 'Measure':
            # Handle MeasureLabel
            measure_codes = meta.meta_dict.get('MeasureCodes', [])
            measure_map = {m['Identifier']: m['Title'] for m in measure_codes}
            data[label_col] = data[dim_col].map(measure_map)
        else:
            # Handle Dimension Labels
            codes_field = f"{dim_col}Codes"
            if codes_field in meta.meta_dict:
                codes = meta.meta_dict[codes_field]
                code_map = {code['Identifier']: code['Title'] for code in codes}
                data[label_col] = data[dim_col].map(code_map)
            else:
                data[label_col] = None  # Or handle as needed
    
    # Reorder columns: place label columns just after the code columns
    cols = list(data.columns)
    for dim_col, label_col in zip(dim_cols, label_cols):
        if dim_col in cols and label_col in cols:
            dim_idx = cols.index(dim_col)
            label_idx = cols.index(label_col)
            if label_idx != dim_idx +1:
                cols.insert(dim_idx +1, cols.pop(label_idx))
    
    data = data[cols]
    
    return data
