import json
from pathlib import Path

import pytest

from scraper.evidence.resolver import EvidenceResolver
from scraper.evidence.types import ResolvedEvidence


@pytest.fixture
def resolver():
    return EvidenceResolver(backend="file_cache")


@pytest.fixture
def sample_evidence_index(tmp_path, monkeypatch):
    """Create a sample evidence index for testing."""
    from scraper.config import get_settings
    
    settings = get_settings()
    index_dir = tmp_path / "cache" / "index"
    index_dir.mkdir(parents=True, exist_ok=True)
    
    # Mock cache dir
    monkeypatch.setattr(settings, "scraper_cache_dir", tmp_path / "cache")
    
    # Create sample evidence index entry
    cache_dir = tmp_path / "cache" / "mediawiki" / "Stephan_Weil" / "123456789" / "parse"
    cache_dir.mkdir(parents=True, exist_ok=True)
    
    metadata = {
        "request_params": {"action": "parse", "page": "Stephan_Weil"},
        "response_headers": {},
        "retrieved_at": "2024-01-01T00:00:00Z",
        "sha256": "abc123def456",
        "url": "https://de.wikipedia.org/w/api.php?action=parse&page=Stephan_Weil",
        "page_title": "Stephan_Weil",
        "page_id": 12345,
        "revision_id": 123456789,
        "endpoint_kind": "parse",
    }
    
    raw_data = {
        "parse": {
            "pageid": 12345,
            "revid": 123456789,
            "text": {
                "*": '<div class="mw-parser-output"><p>Stephan Weil ist ein deutscher Politiker.</p></div>'
            }
        }
    }
    
    (cache_dir / "metadata.json").write_text(json.dumps(metadata, indent=2), encoding="utf-8")
    (cache_dir / "raw.json").write_text(json.dumps(raw_data, indent=2), encoding="utf-8")
    
    # Create evidence index
    evidence_id = "test-evidence-id-123"
    index_file = index_dir / "evidence_index.jsonl"
    index_entry = {
        "evidence_id": evidence_id,
        "source_kind": "mediawiki",
        "cache_metadata_path": str(cache_dir / "metadata.json"),
        "cache_raw_path": str(cache_dir / "raw.json"),
        "page_title": "Stephan_Weil",
        "page_id": 12345,
        "revision_id": 123456789,
        "sha256": "abc123def456",
    }
    
    index_file.write_text(json.dumps(index_entry) + "\n", encoding="utf-8")
    
    return evidence_id


def test_resolve_mediawiki_evidence(resolver, sample_evidence_index, tmp_path, monkeypatch):
    """Test resolving MediaWiki evidence with canonical URL."""
    from scraper.config import get_settings
    
    settings = get_settings()
    monkeypatch.setattr(settings, "scraper_cache_dir", tmp_path / "cache")
    
    resolved = resolver.resolve([sample_evidence_index], with_snippets=False)
    
    assert len(resolved) == 1
    evidence = resolved[0]
    
    assert evidence.evidence_id == sample_evidence_index
    assert evidence.source_kind == "mediawiki"
    assert evidence.page_title == "Stephan_Weil"
    assert evidence.revision_id == 123456789
    assert "oldid=123456789" in evidence.canonical_url
    assert evidence.canonical_url.startswith("https://de.wikipedia.org/w/index.php")


def test_resolve_with_snippet(resolver, sample_evidence_index, tmp_path, monkeypatch):
    """Test resolving evidence with snippet extraction."""
    from scraper.config import get_settings
    
    settings = get_settings()
    monkeypatch.setattr(settings, "scraper_cache_dir", tmp_path / "cache")
    
    resolved = resolver.resolve([sample_evidence_index], with_snippets=True, snippet_max_len=500)
    
    assert len(resolved) == 1
    evidence = resolved[0]
    
    assert evidence.snippet is not None
    assert len(evidence.snippet) > 0
    assert "[" not in evidence.snippet  # No footnote markers
    assert evidence.snippet_source == "lead_paragraph"


def test_resolve_nonexistent_evidence(resolver):
    """Test resolving non-existent evidence ID."""
    resolved = resolver.resolve(["nonexistent-evidence-id"])
    
    assert len(resolved) == 0

