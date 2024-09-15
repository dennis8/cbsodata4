# data.py

from typing import Any, Optional, List
import pandas as pd
from .observations import get_observations
from .metadata import CbsMeta
from .config import DEFAULT_CATALOG, BASE_URL

def get_data(id: str,
             catalog: str = DEFAULT_CATALOG,
             download_dir: Optional[str] = None,
             query: Optional[str] = None,
             select: Optional[List[str]] = None,
             name_measure_columns: bool = True,
             show_progress: Optional[bool] = None,
             verbose: bool = False,
             sep: str = ",",
             base_url: str = BASE_URL,
             **filters: Any) -> pd.DataFrame:
    """Get data from CBS in wide format.

    Args:
        id (str): Identifier of the OpenData table.
        catalog (str): Catalog in which the dataset is to be found.
        download_dir (Optional[str]): Directory to download data. Defaults to temp dir.
        query (Optional[str]): OData4 query in OData syntax.
        select (Optional[List[str]]): Columns to select.
        name_measure_columns (bool): If True, the Title of the measure will be set as column name.
        show_progress (Optional[bool]): If True, shows progress bar.
        verbose (bool): If True, prints additional information.
        sep (str): Separator used in CSV.
        base_url (str): Base URL of the CBS OData4 API.
        **filters (Any): Additional filter parameters.

    Returns:
        pd.DataFrame: DataFrame in wide format with measures as columns.
    """
    # Get observations
    obs = get_observations(id=id,
                           catalog=catalog,
                           download_dir=download_dir,
                           query=query,
                           select=select,
                           sep=sep,
                           show_progress=show_progress,
                           verbose=verbose,
                           include_id=False,
                           base_url=base_url,
                           **filters)

    is_empty = obs.empty

    meta: CbsMeta = obs.attrs.get('meta')

    if meta is None:
        raise ValueError("Metadata is missing in observations.")

    # Pivot the DataFrame from long to wide
    dimensions = meta.meta_dict.get('Dimensions', {}).get('Identifier', [])
    if not dimensions:
        raise ValueError("No dimensions found in metadata.")

    pivot_index = dimensions
    pivot_columns = 'Measure'
    pivot_values = 'Value'

    if is_empty:
        # Create an empty DataFrame with columns as measures
        measures = meta.meta_dict.get('MeasureCodes', [])
        measure_titles = {m['Identifier']: m['Title'] for m in measures}
        d = pd.DataFrame(columns=measure_titles.values())
    else:
        # Pivot
        d = obs.pivot_table(index=pivot_index,
                            columns=pivot_columns,
                            values=pivot_values,
                            aggfunc='first').reset_index()

        # Rename measure columns if required
        if name_measure_columns:
            measure_codes = meta.meta_dict.get('MeasureCodes', [])
            measure_map = {m['Identifier']: m['Title'] for m in measure_codes}
            d.rename(columns=measure_map, inplace=True)
    
    # Attach metadata
    d.attrs['meta'] = meta

    return d
