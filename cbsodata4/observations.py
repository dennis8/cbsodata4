import logging
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow.parquet as pq

from .config import BASE_URL, DEFAULT_CATALOG
from .datasets import get_datasets
from .downloader import download_dataset
from .metadata import get_metadata

logger = logging.getLogger(__name__)


def get_observations(
    id: str,
    catalog: str = DEFAULT_CATALOG,
    download_dir: str | None = None,
    query: str | None = None,
    select: list[str] | None = None,
    include_id: bool = True,
    base_url: str = BASE_URL,
    overwrite: bool = False,
    **filters: dict[str, Any],
) -> pd.DataFrame:
    """
    Retrieve observations from a dataset in long format.

    Fetches data from the specified dataset, applies optional filters and column selection,
    and returns it as a pandas DataFrame.
    """

    toc = get_datasets(catalog=catalog, base_url=base_url)
    if id not in toc["Identifier"].values:
        raise ValueError(f"Table '{id}' cannot be found in catalog '{catalog}'.")

    download_path = Path(download_dir or id)
    if overwrite or not download_path.exists():
        meta = download_dataset(
            id=id,
            download_dir=download_path,
            catalog=catalog,
            query=query,
            select=select,
            base_url=base_url,
            **filters,
        )
    else:
        logger.info(f"Not redownloading files, instead reading from disk at location {download_path}.")
        meta = get_metadata(id=id, catalog=catalog, base_url=base_url)

    observations_path = download_path / "Observations"

    if not observations_path.exists():
        raise FileNotFoundError(f"Observations directory not found at {observations_path}.")

    logger.info(f"Reading parquet files at {observations_path}.")
    obs = pq.read_table(str(observations_path)).to_pandas()

    if not include_id and "Id" in obs.columns:
        obs = obs.drop(columns=["Id"])

    obs.attrs["meta"] = meta

    return obs
