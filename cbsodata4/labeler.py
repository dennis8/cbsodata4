import logging

import pandas as pd

from .metadata import CbsMetadata

logger = logging.getLogger(__name__)


def add_label_columns(data: pd.DataFrame) -> pd.DataFrame:
    """Add descriptive label columns based on metadata."""

    meta: CbsMetadata = data.attrs.get("meta")
    if meta is None:
        logger.error("add_label_columns requires metadata.")
        raise ValueError("add_label_columns requires metadata.")

    label_mappings = meta.get_label_mappings()

    for col, mapping in label_mappings.items():
        if col in data.columns:
            label_col = f"{col}Label"
            data[label_col] = data[col].map(mapping)

    # Reorder columns: place label columns just after the code columns
    cols = list(data.columns)
    label_cols = meta.get_label_columns()
    for dim_col, label_col in zip(["Measure"] + meta.dimension_identifiers, label_cols):
        if dim_col in cols and label_col in cols:
            dim_idx = cols.index(dim_col)
            label_idx = cols.index(label_col)
            if label_idx != dim_idx + 1:
                cols.insert(dim_idx + 1, cols.pop(label_idx))

    return data[cols]
