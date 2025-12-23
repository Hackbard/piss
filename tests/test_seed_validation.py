import pytest
from pathlib import Path

from scraper.cache.mediawiki_cache import validate_seeds


def test_validate_seeds_success(tmp_path, monkeypatch):
    seeds_content = """
nds_lt_17:
  key: nds_lt_17
  page_title: "Test Page"
  expected_time_range:
    start: "2013-01-20"
    end: "2017-11-14"
"""
    seeds_file = tmp_path / "seeds.yaml"
    seeds_file.write_text(seeds_content)

    import scraper.cache.mediawiki_cache as cache_module
    monkeypatch.setattr(cache_module, "SEEDS_FILE", seeds_file)

    validate_seeds()


def test_validate_seeds_missing_field(tmp_path, monkeypatch):
    seeds_content = """
nds_lt_17:
  key: nds_lt_17
  page_title: "Test Page"
"""
    seeds_file = tmp_path / "seeds.yaml"
    seeds_file.write_text(seeds_content)

    import scraper.cache.mediawiki_cache as cache_module
    monkeypatch.setattr(cache_module, "SEEDS_FILE", seeds_file)

    with pytest.raises(ValueError, match="missing required field"):
        validate_seeds()


def test_validate_seeds_duplicate_key(tmp_path, monkeypatch):
    seeds_content = """
nds_lt_17:
  key: nds_lt_17
  page_title: "Test Page"
  expected_time_range:
    start: "2013-01-20"
    end: "2017-11-14"
nds_lt_17_dup:
  key: nds_lt_17
  page_title: "Test Page 2"
  expected_time_range:
    start: "2013-01-20"
    end: "2017-11-14"
"""
    seeds_file = tmp_path / "seeds.yaml"
    seeds_file.write_text(seeds_content)

    import scraper.cache.mediawiki_cache as cache_module
    monkeypatch.setattr(cache_module, "SEEDS_FILE", seeds_file)

    with pytest.raises(ValueError, match="Duplicate seed key"):
        validate_seeds()

