import logging
from functools import cache

import httpx

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
