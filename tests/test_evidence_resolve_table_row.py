import json
from pathlib import Path

import pytest

from scraper.evidence.resolver import EvidenceResolver
from scraper.evidence.types import ResolvedEvidence


@pytest.fixture
def resolver():
    return EvidenceResolver(backend="file_cache")


@pytest.fixture
def sample_table_row_evidence_index(tmp_path, monkeypatch):
    """Create a sample evidence index entry with table_row snippet_ref."""
    from scraper.config import get_settings
    
    settings = get_settings()
    index_dir = tmp_path / "cache" / "index"
    index_dir.mkdir(parents=True, exist_ok=True)
    
    monkeypatch.setattr(settings, "scraper_cache_dir", tmp_path / "cache")
    
    # Create sample cache for member list
    cache_dir = tmp_path / "cache" / "mediawiki" / "Liste_der_Mitglieder_des_Niedersächsischen_Landtages__17__Wahlperiode" / "256198867" / "parse"
    cache_dir.mkdir(parents=True, exist_ok=True)
    
    # Sample HTML with table (Stephan Weil should be in row 0 or 1)
    html_content = '''
    <div class="mw-parser-output">
        <table class="wikitable">
            <tr><th>Name</th><th>Partei</th><th>Wahlkreis</th></tr>
            <tr><td><a href="/wiki/Stephan_Weil">Stephan Weil</a></td><td>SPD</td><td>Hannover-Mitte</td></tr>
            <tr><td><a href="/wiki/Other_Person">Other Person</a></td><td>CDU</td><td>Other District</td></tr>
        </table>
    </div>
    '''
    
    raw_data = {
        "parse": {
            "pageid": 7494713,
            "revid": 256198867,
            "text": {
                "*": html_content
            }
        }
    }
    
    metadata = {
        "request_params": {"action": "parse", "page": "Liste_der_Mitglieder_des_Niedersächsischen_Landtages__17__Wahlperiode"},
        "response_headers": {},
        "retrieved_at": "2024-01-01T00:00:00Z",
        "sha256": "abc123def456",
        "url": "https://de.wikipedia.org/w/api.php?action=parse&page=...",
        "page_title": "Liste_der_Mitglieder_des_Niedersächsischen_Landtages__17__Wahlperiode",
        "page_id": 7494713,
        "revision_id": 256198867,
        "endpoint_kind": "parse",
    }
    
    (cache_dir / "metadata.json").write_text(json.dumps(metadata, indent=2), encoding="utf-8")
    (cache_dir / "raw.json").write_text(json.dumps(raw_data, indent=2), encoding="utf-8")
    
    # Create evidence index with table_row snippet_ref
    evidence_id = "test-evidence-table-row-123"
    index_file = index_dir / "evidence_index.jsonl"
    snippet_ref = {
        "version": 1,
        "type": "table_row",
        "table_index": 0,
        "row_index": 0,  # Stephan Weil is in first data row
        "row_kind": "data",
        "title_hint": "Liste_der_Mitglieder_des_Niedersächsischen_Landtages__17__Wahlperiode",
        "match": {
            "person_title": "Stephan_Weil",
            "name_cell": "Stephan Weil",
        }
    }
    
    index_entry = {
        "evidence_id": evidence_id,
        "source_kind": "mediawiki",
        "cache_metadata_path": str(cache_dir / "metadata.json"),
        "cache_raw_path": str(cache_dir / "raw.json"),
        "page_title": "Liste_der_Mitglieder_des_Niedersächsischen_Landtages__17__Wahlperiode",
        "page_id": 7494713,
        "revision_id": 256198867,
        "sha256": "abc123def456",
        "snippet_ref": snippet_ref,
    }
    
    index_file.write_text(json.dumps(index_entry) + "\n", encoding="utf-8")
    
    return evidence_id


def test_resolve_table_row_snippet(resolver, sample_table_row_evidence_index, tmp_path, monkeypatch):
    """Test resolving evidence with table_row snippet."""
    from scraper.config import get_settings
    
    settings = get_settings()
    monkeypatch.setattr(settings, "scraper_cache_dir", tmp_path / "cache")
    
    resolved = resolver.resolve(
        [sample_table_row_evidence_index],
        with_snippets=True,
        snippet_max_len=500,
        prefer_snippet="table_row",
    )
    
    assert len(resolved) == 1
    evidence = resolved[0]
    
    assert evidence.evidence_id == sample_table_row_evidence_index
    assert evidence.snippet_source == "table_row"
    assert evidence.snippet is not None
    assert "Stephan Weil" in evidence.snippet
    assert "SPD" in evidence.snippet
    assert "[" not in evidence.snippet  # No footnote markers
    assert "oldid=256198867" in evidence.canonical_url
    assert evidence.snippet_ref is not None
    assert evidence.snippet_ref.get("type") == "table_row"
    assert evidence.snippet_ref.get("table_index") == 0
    assert evidence.snippet_ref.get("row_index") == 0


def test_table_row_snippet_contains_name_and_party(resolver, sample_table_row_evidence_index, tmp_path, monkeypatch):
    """Test that table_row snippet contains name and party."""
    from scraper.config import get_settings
    
    settings = get_settings()
    monkeypatch.setattr(settings, "scraper_cache_dir", tmp_path / "cache")
    
    resolved = resolver.resolve(
        [sample_table_row_evidence_index],
        with_snippets=True,
        prefer_snippet="table_row",
    )
    
    assert len(resolved) == 1
    evidence = resolved[0]
    
    snippet = evidence.snippet
    assert snippet is not None
    # Should contain name and party (joined with " | ")
    assert "Stephan Weil" in snippet or "Stephan" in snippet
    assert "SPD" in snippet

