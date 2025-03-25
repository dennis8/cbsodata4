import pandas as pd

from .metadata import CbsMetadata


def add_unit_column(data: pd.DataFrame) -> pd.DataFrame:
    """Add a unit column to observations based on metadata."""
    meta: CbsMetadata = data.attrs.get("meta")
    if meta is None:
        raise ValueError("add_unit_column requires metadata.")

    if "Measure" not in data.columns:
        raise ValueError("Data does not contain 'Measure' column.")

    measure_map = {
        m["Identifier"]: m["Unit"] for m in meta.meta_dict.get("MeasureCodes", [])
    }
    result = data.assign(Unit=data["Measure"].map(measure_map))

    if "Value" in result.columns:
        cols = list(result.columns)
        value_idx = cols.index("Value")
        cols.insert(value_idx + 1, cols.pop(cols.index("Unit")))
        result = result[cols]

    result.attrs = data.attrs.copy()
    return result
