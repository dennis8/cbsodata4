import logging
from pathlib import Path
from typing import Any

import pandas as pd

from .config import BASE_URL, DEFAULT_CATALOG
from .metadata import CbsMetadata
from .observations import get_observations

logger = logging.getLogger(__name__)


def get_wide_data(
    id: str,
    catalog: str = DEFAULT_CATALOG,
    download_dir: str | Path | None = None,
    query: str | None = None,
    select: list[str] | None = None,
    name_measure_columns: bool = True,
    base_url: str = BASE_URL,
    **filters: Any,
) -> pd.DataFrame:
    """Get data from CBS in wide format by pivoting observations, with each Measure as a separate column."""
    obs = get_observations(
        id=id,
        catalog=catalog,
        download_dir=download_dir,
        query=query,
        select=select,
        include_id=False,
        base_url=base_url,
        **filters,
    )

    is_empty = obs.empty
    meta: CbsMetadata = obs.attrs.get("meta")

    if meta is None:
        logger.error("Metadata is missing in observations.")
        raise ValueError("Metadata is missing in observations.")

    dimensions = meta.dimension_identifiers
    if not dimensions:
        logger.error("No dimensions found in metadata.")
        raise ValueError("No dimensions found in metadata.")

    pivot_index = dimensions
    pivot_columns = "Measure"
    pivot_values = "Value"

    if is_empty:
        d = pd.DataFrame(columns=meta.measurecode_mapping.values())
    else:
        d = obs.pivot_table(
            index=pivot_index,
            columns=pivot_columns,
            values=pivot_values,
            aggfunc="first",
        ).reset_index()

        if name_measure_columns:
            d.rename(columns=meta.measurecode_mapping, inplace=True)

    d.attrs["meta"] = meta
    return d
