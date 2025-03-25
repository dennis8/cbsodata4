import logging
from typing import Any

import pandas as pd

from .config import BASE_URL, DEFAULT_CATALOG
from .httpx_client import fetch_json

logger = logging.getLogger(__name__)


class CbsMetadata:
    """Class to handle CBS metadata and provide convenient access methods."""

    def __init__(self, meta_dict: dict[str, Any]):
        self.meta_dict = meta_dict

    @property
    def identifier(self) -> str:
        """Return the identifier of the dataset."""
        return self.meta_dict.get("Properties", {}).get("Identifier", "Unknown")

    @property
    def title(self) -> str:
        """Return the title of the dataset."""
        return self.meta_dict.get("Properties", {}).get("Title", "Unknown")

    @property
    def dimension_identifiers(self) -> list[str]:
        """Return a list of dimension identifiers in the dataset."""
        return [dim["Identifier"] for dim in self.meta_dict.get("Dimensions", [])]

    @property
    def time_dimension_identifiers(self) -> list[str]:
        """Return a list of time dimension identifiers in the dataset."""
        return [
            dim["Identifier"]
            for dim in self.meta_dict.get("Dimensions", [])
            if dim.get("Kind") == "TimeDimension"
        ]

    def get_codes(self) -> list[str]:
        """Return a list of code fields in the metadata."""
        return [
            field
            for field in self.meta_dict
            if field.endswith("Codes") or field.endswith("Groups")
        ]

    @property
    def measurecode_mapping(self) -> dict[str, str]:
        """Returns a dictionary mapping measure identifiers to titles"""
        measure_codes = self.meta_dict.get("MeasureCodes", [])
        return {m["Identifier"]: m["Title"] for m in measure_codes}

    def get_dimension_mapping(self, dim_col: str) -> dict[str, str]:
        """Returns a dictionary mapping dimension identifiers to titles"""
        codes_field = f"{dim_col}Codes"
        codes = self.meta_dict.get(codes_field, [])
        return {code["Identifier"]: code["Title"] for code in codes}

    def get_label_mappings(self) -> dict[str, dict[str, str]]:
        """Returns a dictionary of label mappings for all dimensions and measures"""
        mappings = {"Measure": self.measurecode_mapping}
        for dim in self.dimension_identifiers:
            mappings[dim] = self.get_dimension_mapping(dim)
        return mappings

    def get_label_columns(self) -> list[str]:
        """Returns a list of label column names"""
        return [f"{col}Label" for col in ["Measure"] + self.dimension_identifiers]

    def __repr__(self) -> str:
        return (
            f"cbs odata4: '{self.identifier}':\n"
            f'"{self.title}"\n'
            f"dimensions: {self.dimension_identifiers}\n"
            "For more info use 'meta.meta_dict.keys()' to find out its properties."
        )


def get_metadata(
    id: pd.DataFrame | str,
    catalog: str = DEFAULT_CATALOG,
    base_url: str = BASE_URL,
) -> CbsMetadata:
    """Retrieve the metadata of a publication for the given dataset identifier."""

    if isinstance(id, pd.DataFrame):
        if hasattr(id, "meta") and id.meta is not None:
            return id.meta

    path = f"{base_url}/{catalog}/{id}"
    logger.info(f"Fetching metadata for dataset {id}.")
    meta_data = fetch_json(path)["value"]

    codes = [
        field["name"]
        for field in meta_data
        if field["name"].endswith("Codes") or field["name"].endswith("Groups")
    ]
    names_list = ["Dimensions"] + codes

    meta_dict = {}
    for name in names_list:
        meta_dict[name] = fetch_json(f"{path}/{name}")["value"]

    properties_path = f"{path}/Properties"
    meta_dict["Properties"] = fetch_json(properties_path)
    metadata = CbsMetadata(meta_dict)

    return metadata
