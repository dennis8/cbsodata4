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

    if "Measure" not in data.columns:
        logger.error("Data does not contain 'Measure' column.")
        raise ValueError("Data does not contain 'Measure' column.")

    label_mappings = meta.get_label_mappings()

    for col, mapping in label_mappings.items():
        if col in data.columns:
            label_col = f"{col}Label"
            data[label_col] = data[col].map(mapping)

    # Reorder columns: place label columns just after the code columns
    cols = list(data.columns)
    new_order = []
    for dim_col in ["Measure"] + meta.dimension_identifiers:
        new_order.append(dim_col)
        label_col = f"{dim_col}Label"
        if label_col in cols:
            new_order.append(label_col)
            
    remaining_cols = [col for col in cols if col not in new_order]
    new_order.extend(remaining_cols)

    return data[new_order]
