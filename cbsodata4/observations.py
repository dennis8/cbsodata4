# observations.py

from typing import Any, Optional, List
import pandas as pd
from .utils import read_csv
from .datasets import get_datasets
from .download import download_data
from .metadata import CbsMeta
from .config import BASE_URL, DEFAULT_CATALOG
from pathlib import Path

def get_observations(id: str,
                     catalog: str = DEFAULT_CATALOG,
                     download_dir: Optional[str] = None,
                     query: Optional[str] = None,
                     select: Optional[List[str]] = None,
                     sep: str = ",",
                     show_progress: Optional[bool] = None,
                     verbose: bool = False,
                     include_id: bool = True,
                     base_url: str = BASE_URL,
                     **filters: Any) -> pd.DataFrame:
    """Get observations from a table.

    Args:
        id (str): Identifier of the OpenData table.
        catalog (str): Catalog in which the dataset is to be found.
        download_dir (Optional[str]): Directory to download data. Defaults to temp dir.
        query (Optional[str]): OData4 query in OData syntax.
        select (Optional[List[str]]): Columns to select.
        sep (str): Separator used in CSV.
        show_progress (Optional[bool]): If True, shows progress bar.
        verbose (bool): If True, prints additional information.
        include_id (bool): If False, drops the 'Id' column.
        base_url (str): Base URL of the CBS OData4 API.
        **filters (Any): Additional filter parameters.

    Returns:
        pd.DataFrame: DataFrame with observations.
    """
    # Check if id exists in catalog
    toc = get_datasets(catalog=catalog, verbose=verbose, base_url=base_url)
    if id not in toc['Identifier'].values:
        raise ValueError(f"Table '{id}' cannot be found in catalog '{catalog}'.")

    # Download data
    meta = download_data(id=id,
                         download_dir=download_dir,
                         catalog=catalog,
                         query=query,
                         select=select,
                         sep=sep,
                         show_progress=show_progress,
                         verbose=verbose,
                         base_url=base_url,
                         **filters)

    # Read observations.csv
    download_path = Path(download_dir or id)
    observations_file = download_path / "Observations.csv"
    obs = pd.read_csv(observations_file, sep=sep)

    if not include_id and 'Id' in obs.columns:
        obs = obs.drop(columns=['Id'])

    # Attach metadata
    obs.attrs['meta'] = meta

    return obs
