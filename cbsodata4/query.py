from typing import List, Optional, Union, Any

def eq(column: str, values: Union[str, List[str]]) -> str:
    """Create an OData eq filter.

    Args:
        column (str): Column name.
        values (Union[str, List[str]]): Value or list of values to match.

    Returns:
        str: OData filter string.
    """
    if isinstance(values, list):
        conditions = [f"{column} eq '{v}'" for v in values]
        return "(" + " or ".join(conditions) + ")"
    else:
        return f"{column} eq '{values}'"

def contains(column: str, substring: str) -> str:
    """Create an OData contains filter.

    Args:
        column (str): Column name.
        substring (str): Substring to search for.

    Returns:
        str: OData filter string.
    """
    return f"contains({column}, '{substring}')"

def startswith(column: str, prefix: str) -> str:
    """Create an OData startswith filter.

    Args:
        column (str): Column name.
        prefix (str): Prefix to search for.

    Returns:
        str: OData filter string.
    """
    return f"startswith({column}, '{prefix}')"

def endswith(column: str, suffix: str) -> str:
    """Create an OData endswith filter.

    Args:
        column (str): Column name.
        suffix (str): Suffix to search for.

    Returns:
        str: OData filter string.
    """
    return f"endswith({column}, '{suffix}')"

def and_filter(*filters: str) -> str:
    """Combine multiple filters with 'and'.

    Args:
        *filters (str): Filter strings.

    Returns:
        str: Combined filter string.
    """
    return " and ".join([f"({f})" for f in filters])

def or_filter(*filters: str) -> str:
    """Combine multiple filters with 'or'.

    Args:
        *filters (str): Filter strings.

    Returns:
        str: Combined filter string.
    """
    return " or ".join([f"({f})" for f in filters])

def build_query(filter_str: Optional[str] = None, select: Optional[List[str]] = None) -> str:
    """Build the OData query string.

    Args:
        filter_str (Optional[str]): Filter string.
        select (Optional[List[str]]): List of columns to select.

    Returns:
        str: OData query string.
    """
    query = []
    if filter_str:
        query.append(f"$filter={filter_str}")
    if select:
        select_str = ",".join(select)
        query.append(f"$select={select_str}")
    if query:
        return "?" + "&".join(query)
    else:
        return ""

def get_filter(**filters: Any) -> Optional[str]:
    """Build the OData filter string based on filters.

    Args:
        **filters (Any): Column filters.

    Returns:
        Optional[str]: OData filter string.
    """
    filter_clauses = []
    for column, value in filters.items():
        if isinstance(value, str):
            clause = eq(column, value)
        elif isinstance(value, list):
            clause = eq(column, value)
        elif isinstance(value, dict):
            # Implement more complex filters if needed
            clause = ""
        else:
            clause = ""
        if clause:
            filter_clauses.append(clause)
    if not filter_clauses:
        return None
    return and_filter(*filter_clauses)

def get_query_from_meta(filter_str: Optional[str] = None, select: Optional[List[str]] = None) -> str:
    """Build OData query string.

    Args:
        filter_str (Optional[str]): Filter string.
        select (Optional[List[str]]): List of columns to select.

    Returns:
        str: OData query string.
    """
    return build_query(filter_str, select)
