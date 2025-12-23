import json
from pathlib import Path
from unittest.mock import patch

import pytest

from scraper.sources.dip.ingest import ingest_person_list_sync
from scraper.sources.dip.types import DipPerson


@pytest.fixture
def mock_dip_responses():
    fixture1 = Path(__file__).parent / "fixtures" / "dip" / "person_list_wp19_page1" / "raw.json"
    fixture2 = Path(__file__).parent / "fixtures" / "dip" / "person_list_wp19_page2" / "raw.json"

    response1 = json.loads(fixture1.read_text())
    response2 = json.loads(fixture2.read_text())

    return [response1, response2]


def test_dip_pagination_and_cache(mock_dip_responses, tmp_path, monkeypatch):
    from scraper.config import get_settings
    from scraper.sources.dip.ingest import settings

    monkeypatch.setattr(settings, "scraper_cache_dir", tmp_path)

    async def mock_fetch(*args, **kwargs):
        if not hasattr(mock_fetch, "call_count"):
            mock_fetch.call_count = 0
        mock_fetch.call_count += 1
        return mock_dip_responses[mock_fetch.call_count - 1]

    with patch("scraper.sources.dip.ingest.get_dip_client") as mock_client:
        client_instance = mock_client.return_value
        client_instance.fetch_person_list = mock_fetch

        result = ingest_person_list_sync([19], "test-run-id", force=True)

        assert len(result) == 3
        assert result[0].nachname == "Mustermann"
        assert result[1].nachname == "Schmidt"
        assert result[2].nachname == "Müller"

