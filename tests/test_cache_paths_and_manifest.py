import json
from pathlib import Path

from scraper.cache.mediawiki_cache import (
    get_cache_path,
    get_latest_manifest_path,
    get_manifest_path,
    normalize_title,
)


def test_normalize_title():
    assert normalize_title("Test Page") == "Test_Page"
    assert normalize_title("Test/Page") == "Test_Page"
    assert normalize_title("Test-Page") == "Test-Page"
    assert normalize_title("Test_Page") == "Test_Page"


def test_get_cache_path(tmp_path, monkeypatch):
    from scraper.config import get_settings
    from scraper.cache.mediawiki_cache import settings

    monkeypatch.setattr(settings, "scraper_cache_dir", tmp_path)

    path = get_cache_path("Test Page", 12345, "parse")
    assert "Test_Page" in str(path)
    assert "12345" in str(path)
    assert "parse" in str(path)


def test_get_manifest_path(tmp_path, monkeypatch):
    from scraper.cache.mediawiki_cache import settings

    monkeypatch.setattr(settings, "scraper_cache_dir", tmp_path)

    path = get_manifest_path("test-run-id")
    assert path.parent.name == "manifests"
    assert path.name == "test-run-id.json"

