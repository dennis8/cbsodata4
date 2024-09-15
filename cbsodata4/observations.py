import logging
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow.parquet as pq

from .config import BASE_URL, DEFAULT_CATALOG
from .datasets import get_datasets
from .download import download_dataset

logger = logging.getLogger(__name__)


def get_observations(
    id: str,
    catalog: str = DEFAULT_CATALOG,
    download_dir: str | None = None,
    query: str | None = None,
    select: list[str] | None = None,
    include_id: bool = True,
    base_url: str = BASE_URL,
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

    meta = download_dataset(
        id=id,
        download_dir=download_dir,
        catalog=catalog,
        query=query,
        select=select,
        base_url=base_url,
        **filters,
    )

    observations_dir = Path(download_dir or id) / "Observations"

    if not observations_dir.exists():
        raise FileNotFoundError(f"Observations directory not found at {observations_dir}.")

    logger.info(f"Reading parquet files at {observations_dir}.")
    obs = pq.read_table(str(observations_dir)).to_pandas()

    if not include_id and "Id" in obs.columns:
        obs = obs.drop(columns=["Id"])

    obs.attrs["meta"] = meta

    return obs
