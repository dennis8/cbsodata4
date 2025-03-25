import pandas as pd

from .config import BASE_URL
from .httpx_client import fetch_json


def get_catalogs(base_url: str = BASE_URL) -> pd.DataFrame:
    """Retrieve all (alternative) catalogs of Statistics Netherlands."""
    path = f"{base_url}/Catalogs"
    data = fetch_json(path)
    return pd.DataFrame(data)
