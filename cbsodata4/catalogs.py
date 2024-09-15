from typing import List, Dict
import pandas as pd
from .utils import get_value
from .config import BASE_URL

def get_catalogs(base_url: str = BASE_URL, verbose: bool = False) -> pd.DataFrame:
    """Retrieve all (alternative) catalogs of Statistics Netherlands.

    Args:
        base_url (str): Base URL of the CBS OData4 API.
        verbose (bool): If True, prints additional information.

    Returns:
        pd.DataFrame: DataFrame with the different catalogs available.
    """
    path = f"{base_url}/Catalogs"
    data = get_value(path, singleton=False, verbose=verbose)
    return pd.DataFrame(data)
