# download.py

from typing import Any, Optional, List
from pathlib import Path
import pandas as pd
from .utils import get_value, save_cache, load_cache, download_value, write_csv, write_json
from .metadata import get_metadata, CbsMeta
from .query import get_query_from_meta
from .config import BASE_URL, DEFAULT_CATALOG
from tqdm import tqdm

def download_data(id: str,
                  download_dir: Optional[str] = None,
                  catalog: str = DEFAULT_CATALOG,
                  query: Optional[str] = None,
                  select: Optional[List[str]] = None,
                  sep: str = ",",
                  show_progress: Optional[bool] = None,
                  verbose: bool = False,
                  base_url: str = BASE_URL,
                  **filters: Any) -> CbsMeta:
    """Download observations and metadata to a directory.

    Args:
        id (str): Identifier of publication.
        download_dir (Optional[str]): Directory where files are to be stored. Defaults to id.
        catalog (str): Catalog to download from.
        query (Optional[str]): OData4 query in OData syntax.
        select (Optional[List[str]]): Columns to select.
        sep (str): Separator to be used in writing the data.
        show_progress (Optional[bool]): If True, shows progress bar. Defaults to interactive and not verbose.
        verbose (bool): If True, prints additional information.
        base_url (str): Base URL of the CBS OData4 API.
        **filters (Any): Additional filter parameters.

    Returns:
        CbsMeta: Metadata of table.
    """
    if show_progress is None:
        import sys
        show_progress = sys.stdout.isatty() and not verbose  # interactive and not verbose

    if show_progress and verbose:
        print("Warning: verbose and show_progress can't be used together, show_progress was set to False.")
        show_progress = False

    download_path = Path(download_dir or id)
    download_path.mkdir(parents=True, exist_ok=True)

    # Get metadata
    meta = get_metadata(id, catalog=catalog, base_url=base_url, verbose=verbose)

    # Save metadata to files
    for key, value in meta.meta_dict.items():
        if isinstance(value, list) or isinstance(value, pd.DataFrame):
            df = pd.DataFrame(value)
            path_n = download_path / f"{key}.csv"
            df.to_csv(path_n, sep=sep, na_rep="", index=False)
        elif isinstance(value, dict):
            path_n = download_path / f"{key}.json"
            write_json(value, str(path_n), indent=4)
        else:
            # Handle other types if necessary
            path_n = download_path / f"{key}.json"
            write_json(value, str(path_n), indent=4)

    # Construct Observations path
    observations_path = f"{base_url}/{catalog}/{id}/Observations"
    if query is None:
        constructed_query = get_query_from_meta(select=select, meta=meta, **filters)
        path = f"{observations_path}{constructed_query}"
    else:
        # If query is provided, ignore filters
        if filters:
            print(f"Warning: query argument is used, so ignoring filters {filters}.")
        path = f"{observations_path}?{query}"

    # Encode the URL
    from urllib.parse import urljoin
    path = urljoin(base_url, path)

    output_file = download_path / "Observations.csv"

    if show_progress:
        pb_max = meta.meta_dict.get("Properties", {}).get("ObservationCount", 1000000)  # Default to a large number
        pbar = tqdm(total=pb_max, desc="Downloading observations", unit="rows")

        def progress_callback(df: pd.DataFrame):
            pbar.update(len(df))

    else:
        progress_callback = None

    download_value(str(path),
                  str(output_file),
                  empty_selection=get_empty_dataframe(meta),
                  sep=sep,
                  progress_cb=progress_callback,
                  verbose=verbose)

    if show_progress:
        pbar.close()

    if verbose:
        print(f"The data is in '{download_path}'")

    return meta

def get_empty_dataframe(meta: CbsMeta) -> pd.DataFrame:
    """Create an empty DataFrame with the required structure for empty selections.

    Args:
        meta (CbsMeta): Metadata object.

    Returns:
        pd.DataFrame: Empty DataFrame with necessary columns.
    """
    columns = ['Id', 'Measure', 'ValueAttribute', 'Value']
    dimensions = meta.meta_dict.get('Dimensions', {}).get('Identifier', [])
    for dim in dimensions:
        columns.append(dim)
    empty_df = pd.DataFrame(columns=columns)
    return empty_df
