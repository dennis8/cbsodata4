import json
import logging
from pathlib import Path
from typing import Any

import pandas as pd

from .config import BASE_URL, DEFAULT_CATALOG
from .httpx_client import fetch_json
from .metadata import CbsMetadata, get_metadata
from .query_builder import build_odata_query, construct_filter

logger = logging.getLogger(__name__)


def download_dataset(
    id: str,
    download_dir: str | Path | None = None,
    catalog: str = DEFAULT_CATALOG,
    query: str | None = None,
    select: list[str] | None = None,
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
        path_n = (
            download_path
            / f"{key}.{'parquet' if isinstance(value, (list, pd.DataFrame)) else 'json'}"
        )
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

    download_data_stream(
        url=str(path),
        output_path=str(observations_dir),
        empty_selection=get_empty_dataframe(meta),
    )

    logger.info(f"The data is in '{download_path}'")
    return meta


def get_empty_dataframe(meta: CbsMetadata) -> pd.DataFrame:
    """Create an empty DataFrame with the required structure for empty selections."""
    columns = ["Id", "Measure", "ValueAttribute", "Value"] + meta.dimension_identifiers
    empty_df = pd.DataFrame(columns=columns)
    return empty_df


def download_data_stream(
    url: str,
    output_path: str | Path,
    empty_selection: pd.DataFrame,
) -> None:
    """Download data from an url to output_path folder."""

    def fetch_and_process_data(url: str, partition: int) -> str | None:
        """Fetch data from URL, process it, and write to the output directory."""
        data = fetch_json(url)
        values = data.get("value", empty_selection.to_dict(orient="records"))
        df = pd.DataFrame(values)
        file_path = Path(output_path) / f"partition_{partition}.parquet"
        file_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(file_path, engine="pyarrow", index=False)
        return data.get("@odata.nextLink")

    logger.info(f"Retrieving {url}")
    partition = 0
    next_link = fetch_and_process_data(url, partition)

    while next_link:
        partition += 1
        logger.info(f"Retrieving {next_link}")
        next_link = fetch_and_process_data(next_link, partition)
