import json
import logging
import sys
from pathlib import Path
from typing import Any

import pandas as pd
from tqdm import tqdm

from .config import BASE_URL, DEFAULT_CATALOG
from .metadata import CbsMetadata, get_metadata
from .query import build_odata_query, construct_filter
from .utils import download_data_stream

logger = logging.getLogger(__name__)


def download_dataset(
    id: str,
    download_dir: str | None = None,
    catalog: str = DEFAULT_CATALOG,
    query: str | None = None,
    select: list[str] | None = None,
    show_progress: bool = True,
    base_url: str = BASE_URL,
    **filters: Any,
) -> CbsMetadata:
    """
    Download observations and metadata for a specified dataset, saving them as Parquet files in the given directory.
    """

    download_path = Path(download_dir or id)
    download_path.mkdir(parents=True, exist_ok=True)
    meta = get_metadata(id=id, catalog=catalog, base_url=base_url)

    def save_metadata(key: str, value: Any):
        path_n = download_path / f"{key}.{'parquet' if isinstance(value, (list, pd.DataFrame)) else 'json'}"
        if isinstance(value, (list, pd.DataFrame)):
            pd.DataFrame(value).to_parquet(path_n, engine="pyarrow", index=False)
        else:
            with open(path_n, "w", encoding="utf-8") as f:
                json.dump(value, f, ensure_ascii=False, indent=4)

    for key, value in meta.meta_dict.items():
        save_metadata(key, value)

    observations_path = f"{base_url}/{catalog}/{id}/Observations"
    if query:
        path = f"{observations_path}?{query}"
    else:
        filter_str = construct_filter(**filters)
        odata_query = build_odata_query(filter_str=filter_str, select_fields=select)
        path = f"{observations_path}{odata_query}"

    observations_dir = download_path / "Observations"

    progress_callback = None
    if show_progress and sys.stdout.isatty():
        pbar = tqdm(desc="Downloading observations", unit="rows")

        def update_progress(df: pd.DataFrame):
            pbar.update(len(df))

        progress_callback = update_progress

    download_data_stream(
        url=str(path),
        output_path=str(observations_dir),
        empty_selection=get_empty_dataframe(meta),
        progress_cb=progress_callback,
    )

    if show_progress and sys.stdout.isatty():
        pbar.close()

    logger.info(f"The data is in '{download_path}'")
    return meta


def get_empty_dataframe(meta: CbsMetadata) -> pd.DataFrame:
    """Create an empty DataFrame with the required structure for empty selections."""
    columns = ["Id", "Measure", "ValueAttribute", "Value"]
    dimensions = meta.meta_dict.get("Dimensions", [])
    for dim in dimensions:
        dim_id = dim.get("Identifier")
        if dim_id:
            columns.append(dim_id)
    empty_df = pd.DataFrame(columns=columns)
    return empty_df
