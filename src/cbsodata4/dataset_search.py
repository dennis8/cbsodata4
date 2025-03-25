import logging
from urllib.parse import urlencode

import pandas as pd

from .config import BASE_URL, DEFAULT_CATALOG, DEFAULT_LANGUAGE, SEARCH_URL
from .datasets import get_datasets
from .httpx_client import fetch_json

logger = logging.getLogger(__name__)


def search_datasets(
    query: str,
    catalog: str = DEFAULT_CATALOG,
    language: str = DEFAULT_LANGUAGE,
    convert_dates: bool = True,
    base_url: str = BASE_URL,
) -> pd.DataFrame:
    """Search an OpenData table using free text search.
    Searches datasets using a free-text query and returns matching datasets with relevance scores.

    Returns a DataFrame in same format as get_datasets() plus 'rel' and 'url' columns with search scores.
    """
    params = {
        "query": query,
        "spelling_correction": "true",
        "language": language,
        "sort_by": "relevance",
        "highlight": "false",
    }
    query_string = urlencode(params)
    url = f"{SEARCH_URL}?{query_string}"

    logger.info(f"Searching datasets with query: {query}")

    res = fetch_json(url)
    res_results = res.get("results", [])
    res_tables = pd.DataFrame(
        [
            {"unique_id": r.get("unique_id"), "rel": r.get("rel"), "url": r.get("url")}
            for r in res_results
            if r.get("document_type") == "table"
        ]
    )

    ds = get_datasets(catalog=catalog, convert_dates=convert_dates, base_url=base_url)
    res_ds = ds.merge(
        res_tables, how="left", left_on="Identifier", right_on="unique_id"
    )
    res_ds = res_ds.dropna(subset=["unique_id"])

    return res_ds
