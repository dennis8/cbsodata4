from typing import Any

from .config import BASE_URL
from .httpx_client import fetch_json


def get_catalogs(base_url: str = BASE_URL) -> dict[str, Any]:
    """Retrieve all (alternative) catalogs of Statistics Netherlands."""
    path = f"{base_url}/Catalogs"
    data = fetch_json(path)
    return data
