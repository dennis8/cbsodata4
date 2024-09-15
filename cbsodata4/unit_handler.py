import logging

import pandas as pd

from .metadata import CbsMetadata

logger = logging.getLogger(__name__)


def add_unit_column(data: pd.DataFrame) -> pd.DataFrame:
    """Add a unit column to observations based on metadata.

    Retrieves the Unit for each Measure from MeasureCodes in metadata and adds it as a 'Unit' column.

    Args:
        data (pd.DataFrame): DataFrame retrieved using get_observations().

    Returns:
        pd.DataFrame: Original DataFrame with an additional 'Unit' column.
    """
    meta: CbsMetadata = data.attrs.get("meta")
    if meta is None:
        logger.error("add_unit_column requires metadata.")
        raise ValueError("add_unit_column requires metadata.")

    if "Measure" not in data.columns:
        logger.error("Data does not contain 'Measure' column.")
        raise ValueError("Data does not contain 'Measure' column.")

    measure_codes = meta.meta_dict.get("MeasureCodes", [])
    measure_map = {m["Identifier"]: m["Unit"] for m in measure_codes}

    data["Unit"] = data["Measure"].map(measure_map)

    # Reorder columns: place 'Unit' after 'Value' if exists
    if "Value" in data.columns and "Unit" in data.columns:
        value_idx = data.columns.get_loc("Value")
        cols = list(data.columns)
        cols.insert(value_idx + 1, cols.pop(cols.index("Unit")))
        data = data[cols]

    return data
