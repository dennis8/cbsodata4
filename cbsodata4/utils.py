import logging
from functools import cache
from pathlib import Path
from typing import Callable

import httpx
import pandas as pd

logger = logging.getLogger(__name__)


@cache
def fetch_json(path: str) -> dict:
    """Retrieve JSON data from a URL."""
    logger.info(f"Retrieving {path}")
    try:
        response = httpx.get(path)
        response.raise_for_status()
        return response.json()
    except httpx.HTTPError as e:
        logger.error(f"HTTP error while fetching {path}: {e}")
        raise


def download_data_stream(
    url: str,
    output_path: str | Path,
    empty_selection: pd.DataFrame,
    progress_cb: Callable[[pd.DataFrame], None] | None = None,
) -> None:
    """Download value data from a path to output_file.

    Args:
        url (str): API URL to download data from.
        output_path (str | Path): Directory path to store partitioned Parquet files.
        empty_selection (pd.DataFrame): DataFrame to write if selection is empty.
        progress_cb (Any): Callback function for progress updates.
    """

    def fetch_and_process_data(url: str, partition: int) -> str | None:
        """Fetch data from URL, process it, and write to the output directory."""
        data = fetch_json(url)
        values = data.get("value", empty_selection.to_dict(orient="records"))
        df = pd.DataFrame(values)
        file_path = Path(output_path) / f"partition_{partition}.parquet"
        file_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(file_path, engine="pyarrow", index=False)

        if progress_cb:
            progress_cb(df)

        return data.get("@odata.nextLink")

    try:
        logger.info(f"Retrieving {url}")
        partition = 0
        next_link = fetch_and_process_data(url, partition)

        while next_link:
            partition += 1
            logger.info(f"Retrieving {next_link}")
            next_link = fetch_and_process_data(next_link, partition)

    except httpx.HTTPError as e:
        logger.error(f"HTTP error during data download from {url}: {e}")
        raise
    except Exception as e:
        logger.error(f"Error during data download: {e}")
        raise