import logging
from typing import Any, List, Optional

import pandas as pd

from .config import BASE_URL, DEFAULT_CATALOG
from .metadata import CbsMetadata
from .observations import get_observations

logger = logging.getLogger(__name__)


def get_wide_data(
    id: str,
    catalog: str = DEFAULT_CATALOG,
    download_dir: Optional[str] = None,
    query: Optional[str] = None,
    select: Optional[List[str]] = None,
    name_measure_columns: bool = True,
    show_progress: Optional[bool] = None,
    base_url: str = BASE_URL,
    **filters: Any,
) -> pd.DataFrame:
    """Get data from CBS in wide format by pivoting observations.

    Retrieves observations and pivots them to wide format, with each Measure as a separate column.

    Args:
        id (str): Identifier of the OpenData table.
        catalog (str): Catalog in which the dataset is to be found.
        download_dir (Optional[str]): Directory to download data. Defaults to temp dir.
        query (Optional[str]): OData4 query in OData syntax.
        select (Optional[List[str]]): Columns to select.
        name_measure_columns (bool): If True, the Title of the measure will be set as column name.
        show_progress (Optional[bool]): If True, shows progress bar.
        base_url (str): Base URL of the CBS OData4 API.
        **filters (Any): Additional filter parameters.

    Returns:
        pd.DataFrame: DataFrame in wide format with measures as columns.
    """
    obs = get_observations(
        id=id,
        catalog=catalog,
        download_dir=download_dir,
        query=query,
        select=select,
        show_progress=show_progress,
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
            index=pivot_index, columns=pivot_columns, values=pivot_values, aggfunc="first"
        ).reset_index()

        if name_measure_columns:
            d.rename(columns=meta.measurecode_mapping, inplace=True)

    d.attrs["meta"] = meta
    return d
