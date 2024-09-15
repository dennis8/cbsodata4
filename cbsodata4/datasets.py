from typing import Optional
import pandas as pd
from .utils import get_value, save_cache, load_cache
from .config import BASE_URL, DEFAULT_CATALOG

def get_datasets(catalog: Optional[str] = DEFAULT_CATALOG,
                convert_dates: bool = True,
                verbose: bool = False,
                base_url: str = BASE_URL) -> pd.DataFrame:
    """Get available datasets from CBS.

    Args:
        catalog (Optional[str]): Only show the datasets from that catalog. If None, all datasets of all catalogs will be returned.
        convert_dates (bool): Converts date columns to datetime type.
        verbose (bool): If True, prints additional information.
        base_url (str): Base URL of the CBS OData4 API.

    Returns:
        pd.DataFrame: DataFrame with publication metadata of tables.
    """
    cache_filename = "datasets.pkl"
    ds = load_cache(cache_filename)

    if ds is not None:
        if verbose:
            print(f"Reading datasets from cache: {cache_filename}")
    else:
        path = f"{base_url}/Datasets"
        data = get_value(path, singleton=False, verbose=verbose)
        ds = pd.DataFrame(data)
        save_cache(ds, cache_filename)

    if catalog is not None:
        ds = ds[ds['Catalog'].isin([catalog])]

    if convert_dates:
        for date_col in ['Modified', 'ObservationsModified']:
            if date_col in ds.columns:
                ds[date_col] = pd.to_datetime(ds[date_col], errors='coerce')

    return ds

# Alias for get_datasets
get_toc = get_datasets
