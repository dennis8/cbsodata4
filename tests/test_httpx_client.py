from unittest.mock import MagicMock, patch

import httpx
import pytest

from cbsodata4.httpx_client import fetch_json


@patch("cbsodata4.httpx_client.httpx.get")
def test_fetch_json_success(mock_get):
    """Test successful JSON data retrieval."""
    mock_response = MagicMock()
    mock_response.json.return_value = {"data": "test_data"}
    mock_response.raise_for_status.return_value = None
    mock_get.return_value = mock_response

    result = fetch_json("https://test.url")

    mock_get.assert_called_once_with("https://test.url")
    assert result == {"data": "test_data"}


@patch("cbsodata4.httpx_client.httpx.get")
def test_fetch_json_http_error(mock_get):
    """Test handling of HTTP errors."""
    # Directly configure the mock to raise an exception
    mock_response = MagicMock()
    mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
        "404 error",
        request=httpx.Request("GET", "https://test.url"),
        response=httpx.Response(404, request=httpx.Request("GET", "https://test.url")),
    )
    mock_get.return_value = mock_response

    # Clear the cache to ensure a clean test
    fetch_json.cache_clear()

    with pytest.raises(httpx.HTTPStatusError):
        fetch_json("https://test.url")


@patch("cbsodata4.httpx_client.httpx.get")
def test_fetch_json_caching(mock_get):
    """Test that the fetch_json function caches results."""
    # Clear the cache to ensure a clean test
    fetch_json.cache_clear()

    mock_response = MagicMock()
    mock_response.json.return_value = {"data": "test_data"}
    mock_response.raise_for_status.return_value = None
    mock_get.return_value = mock_response

    # Call twice with the same URL
    result1 = fetch_json("https://test.url")
    result2 = fetch_json("https://test.url")

    # Should only make one actual HTTP request
    mock_get.assert_called_once_with("https://test.url")
    assert result1 == result2 == {"data": "test_data"}
