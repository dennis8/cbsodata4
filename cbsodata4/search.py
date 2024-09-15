# search.py

from typing import Any, Optional
import pandas as pd
from .utils import get_value
from .datasets import get_datasets
from .config import SEARCH_URL, BASE_URL
from urllib.parse import urlencode

def search_tables(query: str,
                 catalog: str = "CBS",
                 language: str = "nl-nl",
                 convert_dates: bool = True,
                 verbose: bool = False,
                 base_url: str = BASE_URL) -> pd.DataFrame:
    """Search an OpenData table using free text search.

    Args:
        query (str): Text to search for.
        catalog (str): Catalog to search in.
        language (str): Language of the catalog, currently only Dutch.
        convert_dates (bool): Converts date columns to datetime type.
        verbose (bool): If True, prints additional information.
        base_url (str): Base URL of the CBS OData4 API.

    Returns:
        pd.DataFrame: DataFrame same format as get_datasets() plus extra 'rel' and 'url' columns with search score.
    """
    params = {
        'query': query,
        'spelling_correction': 'true',
        'language': language,
        'sort_by': 'relevance',
        'highlight': 'false'
    }
    url = f"{SEARCH_URL}?{urlencode(params)}"

    res = get_value(url, singleton=True, verbose=verbose)

    res_results = res.get('results', [])
    res_tables = pd.DataFrame([{
        'unique_id': r.get('unique_id'),
        'rel': r.get('rel'),
        'url': r.get('url')
    } for r in res_results if r.get('document_type') == 'table'])

    ds = get_datasets(catalog=catalog, verbose=verbose, convert_dates=convert_dates, base_url=base_url)

    res_ds = ds.merge(res_tables, how='left', left_on='Identifier', right_on='unique_id')
    res_ds = res_ds.dropna(subset=['unique_id'])

    return res_ds
