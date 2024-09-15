import logging
from functools import cache
from typing import Any

from .config import BASE_URL, DEFAULT_CATALOG
from .utils import fetch_json

logger = logging.getLogger(__name__)


class CbsMetadata:
    """Metadata object for CBS datasets."""

    def __init__(self, meta_dict: dict[str, Any]):
        self.meta_dict = meta_dict
        for key, value in meta_dict.items():
            setattr(self, key, value)

    def __repr__(self) -> str:
        identifier = self.meta_dict.get("Properties", {}).get("Identifier", "Unknown")
        title = self.meta_dict.get("Properties", {}).get("Title", "Unknown")
        dimensions = ", ".join([dim["Identifier"] for dim in self.meta_dict.get("Dimensions", [])])
        return (
            f"cbs odata4: '{identifier}':\n"
            f'"{title}"\n'
            f"dimensions: {dimensions}\n"
            "For more info use 'str(meta)' or 'meta.meta_dict.keys()' to find out its properties."
        )


@cache
def get_metadata(
    id: Any,
    catalog: str = DEFAULT_CATALOG,
    base_url: str = BASE_URL,
) -> CbsMetadata:
    """Retrieve the metadata of a publication for the given dataset identifier."""
    # Check if 'id' has 'meta' attribute
    if hasattr(id, "meta") and id.meta is not None:
        return id.meta

    path = f"{base_url}/{catalog}/{id}"
    logger.info(f"Fetching metadata for dataset {id}.")
    meta_data = fetch_json(path)["value"]

    # Extract codes and groups
    codes = [
        field["name"] for field in meta_data if field["name"].endswith("Codes") or field["name"].endswith("Groups")
    ]
    names_list = ["Dimensions"] + codes

    meta_dict = {}
    for name in names_list:
        meta_dict[name] = fetch_json(f"{path}/{name}")["value"]

    properties_path = f"{path}/Properties"
    meta_dict["Properties"] = fetch_json(properties_path)
    metadata = CbsMetadata(meta_dict)

    return metadata
