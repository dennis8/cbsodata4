import logging
from functools import cache

import pandas as pd

from .config import BASE_URL, DEFAULT_CATALOG
from .utils import fetch_json

logger = logging.getLogger(__name__)


@cache
def get_datasets(convert_dates: bool = True, catalog: str = DEFAULT_CATALOG, base_url: str = BASE_URL) -> pd.DataFrame:
    """
    Get DataFrame with available datasets and publication metadata from CBS.
    Retrieves datasets from the specified catalog, optionally converting date columns to datetime.
    """
    logger.info("Fetching datasets from API.")

    path = f"{base_url}/Datasets"
    data = fetch_json(path)
    ds = pd.DataFrame(data["value"])

    if catalog is not None:
        ds = ds[ds["Catalog"] == catalog]

    if convert_dates:
        for date_col in ["Modified", "ObservationsModified"]:
            if date_col in ds.columns:
                ds[date_col] = pd.to_datetime(ds[date_col], errors="coerce")

    return ds
