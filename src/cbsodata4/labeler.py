import logging

import pandas as pd

from .metadata import CbsMetadata

logger = logging.getLogger(__name__)


def add_label_columns(data: pd.DataFrame) -> pd.DataFrame:
    """Add descriptive label columns to the data based on metadata mappings."""
    meta: CbsMetadata = data.attrs.get("meta")
    if meta is None:
        raise ValueError("add_label_columns requires metadata.")

    original_cols = list(data.columns)
    new_columns = {}

    for col, mapping in meta.get_label_mappings().items():
        if col in data.columns:
            new_columns[f"{col}Label"] = data[col].map(mapping)
        elif col != "Measure":
            raise ValueError(
                f"Data does not contain column '{col}' required for labeling."
            )

    result = data.assign(**new_columns)

    new_order = []
    for col in original_cols:
        new_order.append(col)
        label_col = f"{col}Label"
        if label_col in new_columns:
            new_order.append(label_col)

    result = result[new_order]
    result.attrs = data.attrs.copy()

    return result
