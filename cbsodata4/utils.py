import logging
import os
import pickle
from pathlib import Path
from typing import Any, Optional

import httpx
import pandas as pd
import json
from tqdm import tqdm

def get_cache_dir() -> Path:
    """Get the cache directory path."""
    cache_dir = Path.home() / ".cbsodata4py" / "cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir

def save_cache(obj: Any, filename: str) -> None:
    """Save an object to cache."""
    cache_path = get_cache_dir() / filename
    with open(cache_path, "wb") as f:
        pickle.dump(obj, f)

def load_cache(filename: str) -> Optional[Any]:
    """Load an object from cache."""
    cache_path = get_cache_dir() / filename
    if cache_path.exists():
        with open(cache_path, "rb") as f:
            return pickle.load(f)
    return None

def url_encode(s: str) -> str:
    """URL encode a string."""
    from urllib.parse import quote
    return quote(s)

def read_json(path: str, **kwargs) -> Any:
    """Read JSON from a URL."""
    response = httpx.get(path)
    response.raise_for_status()
    return response.json()

def write_json(data: Any, path: str, **kwargs) -> None:
    """Write JSON data to a file."""
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

def write_csv(df: pd.DataFrame, path: str, sep: str = ",", **kwargs) -> None:
    """Write DataFrame to CSV."""
    df.to_csv(path, sep=sep, index=False)

def read_csv(path: str, sep: str = ",", **kwargs) -> pd.DataFrame:
    """Read CSV into DataFrame."""
    return pd.read_csv(path, sep=sep)

def get_value(path: str, singleton: bool = False) -> Any:
    """Retrieve JSON data from a URL."""
    logging.info(f"Retrieving {path}")
    data = read_json(path)
    if singleton:
        return data
    else:
        return data.get('value', data)

def download_value(path: str,
                  output_file: str,
                  empty_selection: pd.DataFrame,
                  sep: str = ",",
                  progress_cb: Any = None) -> None:
    """Download value data from a path to output_file."""
    with httpx.stream("GET", path) as response:
        response.raise_for_status()
        data = response.json()
        values = data.get('value', empty_selection.to_dict(orient='records'))

        # Write initial data
        df = pd.DataFrame(values)
        df.to_csv(output_file, sep=sep, index=False, mode='w', header=True)

        if progress_cb:
            progress_cb(df)

        next_link = data.get('@odata.nextLink')

        while next_link:
            logging.info(f"Retrieving {next_link}")
            with httpx.stream("GET", next_link) as resp:
                resp.raise_for_status()
                data = resp.json()
                values = data.get('value', empty_selection.to_dict(orient='records'))
                df = pd.DataFrame(values)
                df.to_csv(output_file, sep=sep, index=False, mode='a', header=False)

                if progress_cb:
                    progress_cb(df)

                next_link = data.get('@odata.nextLink')
