import pytest

from cbsodata4.query_builder import (
    build_contains_filter,
    build_endswith_filter,
    build_eq_filter,
    build_odata_query,
    build_startswith_filter,
    combine_filters_with_and,
    combine_filters_with_or,
    construct_filter,
)


def test_build_eq_filter_single():
    assert build_eq_filter("Name", "John") == "Name eq 'John'"


def test_build_eq_filter_list():
    expected = "(Name eq 'John' or Name eq 'Jane')"
    assert build_eq_filter("Name", ["John", "Jane"]) == expected


def test_build_contains_filter():
    assert build_contains_filter("Description", "test") == "contains(Description, 'test')"


def test_build_startswith_filter():
    assert build_startswith_filter("Code", "A") == "startswith(Code, 'A')"


def test_build_endswith_filter():
    assert build_endswith_filter("Code", "Z") == "endswith(Code, 'Z')"


def test_combine_filters_with_and():
    filters = ["Name eq 'John'", "Age eq '30'"]
    expected = "(Name eq 'John') and (Age eq '30')"
    assert combine_filters_with_and(filters) == expected


def test_combine_filters_with_or():
    filters = ["Name eq 'John'", "Age eq '30'"]
    expected = "(Name eq 'John') or (Age eq '30')"
    assert combine_filters_with_or(filters) == expected


def test_build_odata_query_with_filter_and_select():
    filter_str = "Name eq 'John'"
    select_fields = ["Name", "Age"]
    expected = "?$filter=Name eq 'John'&$select=Name,Age"
    assert build_odata_query(filter_str, select_fields) == expected


def test_build_odata_query_only_filter():
    filter_str = "Name eq 'John'"
    expected = "?$filter=Name eq 'John'"
    assert build_odata_query(filter_str) == expected


def test_build_odata_query_only_select():
    select_fields = ["Name", "Age"]
    expected = "?$select=Name,Age"
    assert build_odata_query(select_fields=select_fields) == expected


def test_build_odata_query_no_params():
    assert build_odata_query() == ""


def test_construct_filter_single():
    filter_str = construct_filter(Name="John")
    assert filter_str == "(Name eq 'John')"


def test_construct_filter_multiple():
    filter_str = construct_filter(Name=["John", "Jane"], Age="30")
    expected = "((Name eq 'John' or Name eq 'Jane')) and (Age eq '30')"
    assert filter_str == expected


def test_construct_filter_empty():
    assert construct_filter() is None


def test_construct_filter_unsupported_type():
    with pytest.raises(NotImplementedError, match="column filter for <class 'int'> is not supported"):
        construct_filter(Name=123)
