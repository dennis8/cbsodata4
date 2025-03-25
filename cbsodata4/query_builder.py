def build_eq_filter(column: str, values: str | list[str]) -> str:
    """Create an OData eq filter for a column with given values."""
    if isinstance(values, list):
        conditions = [f"{column} eq '{value}'" for value in values]
        return "(" + " or ".join(conditions) + ")"
    else:
        return f"{column} eq '{values}'"


def build_contains_filter(column: str, substring: str) -> str:
    """Create an OData contains filter for a column with a given substring."""
    return f"contains({column}, '{substring}')"


def build_startswith_filter(column: str, prefix: str) -> str:
    """Create an OData startswith filter for a column with a given prefix."""
    return f"startswith({column}, '{prefix}')"


def build_endswith_filter(column: str, suffix: str) -> str:
    """Create an OData endswith filter for a column with a given suffix."""
    return f"endswith({column}, '{suffix}')"


def combine_filters_with_and(filters: list[str]) -> str:
    """Combine multiple filter strings with 'and'."""
    return " and ".join([f"({filter_str})" for filter_str in filters])


def combine_filters_with_or(filters: list[str]) -> str:
    """Combine multiple filter strings with 'or'."""
    return " or ".join([f"({filter_str})" for filter_str in filters])


def build_odata_query(filter_str: str | None = None, select_fields: list[str] | None = None) -> str:
    """Build the OData query string with optional filters and select fields."""
    query_parts = []
    if filter_str:
        query_parts.append(f"$filter={filter_str}")
    if select_fields:
        select_str = ",".join(select_fields)
        query_parts.append(f"$select={select_str}")
    if query_parts:
        return "?" + "&".join(query_parts)
    return ""


def construct_filter(**column_filters: dict[str, str | list]) -> str | None:
    """Construct the OData filter string based on column filters."""
    filter_clauses = []
    for column, value in column_filters.items():
        if isinstance(value, (str, list)):
            clause = build_eq_filter(column, value)
            filter_clauses.append(clause)
        else:
            raise NotImplementedError(f"column filter for {type(value)} is not supported")
    if not filter_clauses:
        return None
    return combine_filters_with_and(filter_clauses)
