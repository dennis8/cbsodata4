from unittest.mock import patch

from cbsodata4.catalogs import get_catalogs


@patch("cbsodata4.catalogs.fetch_json")
def test_get_catalogs(mock_fetch_json):
    """Test retrieving catalogs."""
    mock_fetch_json.return_value = {
        "@odata.context": "https://datasets.cbs.nl/odata/v1/$metadata#Catalogs",
        "value": [
            {
                "Identifier": "CBS",
                "Index": 1,
                "Title": "CBS databank",
                "Description": "Catalogus van het CBS",
                "Publisher": "https://standaarden.overheid.nl/owms/terms/Centraal_Bureau_voor_de_Statistiek",
                "Language": "nl",
                "License": "https://creativecommons.org/licenses/by/4.0/",
                "Homepage": "https://www.cbs.nl/",
                "Authority": "https://standaarden.overheid.nl/owms/terms/Centraal_Bureau_voor_de_Statistiek",
                "ContactPoint": "infoservice@cbs.nl",
            }
        ],
    }

    result = get_catalogs()

    mock_fetch_json.assert_called_once()
    assert isinstance(result, dict)
    assert "@odata.context" in result
    assert "value" in result
    assert isinstance(result["value"], list)
    assert len(result["value"]) == 1
    assert result["value"][0]["Identifier"] == "CBS"
    assert result["value"][0]["Index"] == 1
