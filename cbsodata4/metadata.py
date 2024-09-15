import logging
from typing import Any, Dict
from functools import cache
import pandas as pd
from .utils import get_value, save_cache, load_cache
from .config import BASE_URL, DEFAULT_CATALOG

class CbsMeta:
    """Metadata object for CBS datasets."""

    def __init__(self, meta_dict: Dict[str, Any]):
        self.meta_dict = meta_dict
        for key, value in meta_dict.items():
            setattr(self, key, value)

    def __repr__(self):
        identifier = self.meta_dict.get('Properties', {}).get('Identifier', 'Unknown')
        title = self.meta_dict.get('Properties', {}).get('Title', 'Unknown')
        dimensions = ", ".join([dim['Identifier'] for dim in self.meta_dict.get('Dimensions', {}).get('Identifier', [])])
        return (f"cbs odata4: '{identifier}':\n"
                f'"{title}"\n'
                f"dimensions: {dimensions}\n"
                "For more info use 'str(meta)' or 'meta.meta_dict.keys()' to find out its properties.")

@cache
def get_metadata(id: Any,
                catalog: str = DEFAULT_CATALOG,
                base_url: str = BASE_URL) -> CbsMeta:
    """Retrieve the metadata of a publication.

    Args:
        id (Any): Identifier of publication or data retrieved with get_data()/get_observations().
        catalog (str): Catalog from the set of get_catalogs().
        base_url (str): Base URL of the CBS OData4 API.
        verbose (bool): If True, prints additional information.

    Returns:
        CbsMeta: Metadata object containing all metadata properties of the dataset.
    """
    # Check if 'id' has 'meta' attribute
    if hasattr(id, 'meta') and id.meta is not None:
        return id.meta

    path = f"{base_url}/{catalog}/{id}"

    # Retrieve metadata
    meta_data = get_value(path)

    # Extract codes
    codes = [field['name'] for field in meta_data['value'] if field['name'].endswith("Codes") or field['name'].endswith("Groups")]
    
    names_list = ["Dimensions"] + [field['name'] for field in meta_data['value'] if field['name'].endswith("Codes") or field['name'].endswith("Groups")]

    m = {}
    for name in names_list:
        sub_path = f"{path}/{name}"
        m[name] = get_value(sub_path)

    # Get Properties
    properties_path = f"{path}/Properties"
    m["Properties"] = get_value(properties_path)

    m = CbsMeta(m)

    return m
