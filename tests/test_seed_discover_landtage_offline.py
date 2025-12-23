import json
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
import yaml

from scraper.seeds.discover_landtage import discover_landtage_seeds, extract_legislature_number, validate_member_list_table


def test_extract_legislature_number():
    """Test extraction of legislature number from titles."""
    assert extract_legislature_number("Liste der Mitglieder des Niedersächsischen Landtages (17. Wahlperiode)") == 17
    assert extract_legislature_number("Liste der Mitglieder des Niedersächsischen Landtages (18. Wahlperiode)") == 18
    assert extract_legislature_number("Liste (1. Wahlperiode)") == 1
    assert extract_legislature_number("Liste ohne Wahlperiode") is None
    assert extract_legislature_number("Liste (Wahlperiode 19)") is None  # Wrong format


def test_validate_member_list_table_valid():
    """Test validation of valid member list table."""
    html = """
    <table>
        <tr>
            <th>Name</th>
            <th>Partei</th>
            <th>Wahlkreis</th>
        </tr>
        <tr>
            <td><a href="/wiki/Person1">Person 1</a></td>
            <td>CDU</td>
            <td>WK 1</td>
        </tr>
        <tr>
            <td><a href="/wiki/Person2">Person 2</a></td>
            <td>SPD</td>
            <td>WK 2</td>
        </tr>
    </table>
    """
    is_valid, reason = validate_member_list_table(html, ["Name", "Partei", "Wahlkreis"])
    assert is_valid is True
    assert reason is None


def test_validate_member_list_table_invalid():
    """Test validation of invalid table (missing required columns)."""
    html = """
    <table>
        <tr>
            <th>Name</th>
            <th>Datum</th>
        </tr>
        <tr>
            <td>Person 1</td>
            <td>2020-01-01</td>
        </tr>
    </table>
    """
    is_valid, reason = validate_member_list_table(html, ["Name", "Partei", "Wahlkreis"])
    assert is_valid is False
    assert reason is not None


@pytest.mark.asyncio
async def test_discover_landtage_seeds_offline():
    """Test seed discovery with offline fixtures."""
    fixtures_dir = Path(__file__).parent / "fixtures" / "mediawiki"
    
    # Load fixtures
    search_fixture = json.loads((fixtures_dir / "search" / "landtag_search" / "raw.json").read_text())
    query_fixture = json.loads((fixtures_dir / "query" / "landtag_query" / "raw.json").read_text())
    parse_fixture = json.loads((fixtures_dir / "parse" / "landtag_parse" / "raw.json").read_text())
    
    registry_path = Path(__file__).parent.parent / "config" / "landtage_registry.yaml"
    output_path = Path("/tmp/test_seeds_landtage.yaml")
    
    with patch("scraper.seeds.discover_landtage.get_client") as mock_client:
        client_mock = AsyncMock()
        mock_client.return_value = client_mock
        
        # Mock search
        client_mock.fetch_search = AsyncMock(return_value=search_fixture)
        
        # Mock query
        client_mock.fetch_query = AsyncMock(return_value=query_fixture)
        
        # Mock parse
        client_mock.fetch_parse = AsyncMock(return_value=parse_fixture)
        
        # Mock cache paths to return non-existent (force fetch)
        with patch("scraper.seeds.discover_landtage.settings") as mock_settings:
            mock_settings.scraper_cache_dir = Path("/tmp/test_cache")
            mock_settings.scraper_registry_path = registry_path
            mock_settings.scraper_seeds_landtage_path = output_path
            
            manifest = await discover_landtage_seeds(
                registry_path=registry_path,
                output_path=output_path,
                pin_revisions=True,
                force=True,
            )
            
            assert manifest["status"] == "success"
            assert manifest["seed_count"] > 0
            assert output_path.exists()
            
            # Validate output YAML
            with open(output_path, "r", encoding="utf-8") as f:
                seeds = yaml.safe_load(f)
            
            assert isinstance(seeds, dict)
            for seed_key, seed_data in seeds.items():
                assert "key" in seed_data
                assert "page_title" in seed_data
                assert "hints" in seed_data
                assert seed_data["hints"]["legislature_number"] is not None

