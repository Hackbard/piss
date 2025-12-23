import json
from pathlib import Path

import pytest
import yaml

from scraper.evidence.formatters import (
    format_resolved_evidence_json,
    format_resolved_evidence_markdown,
    format_resolved_evidence_yaml,
)
from scraper.evidence.types import ResolvedEvidence


@pytest.fixture
def sample_resolved_evidence():
    """Create sample ResolvedEvidence for testing."""
    return ResolvedEvidence(
        evidence_id="test-evidence-123",
        source_kind="mediawiki",
        page_title="Stephan_Weil",
        page_id=12345,
        revision_id=123456789,
        retrieved_at_utc="2024-01-01T00:00:00Z",
        sha256="abc123def456",
        source_url="https://de.wikipedia.org/w/api.php?action=parse&page=Stephan_Weil",
        canonical_url="https://de.wikipedia.org/w/index.php?title=Stephan_Weil&oldid=123456789",
        cache_metadata_path="/cache/metadata.json",
        cache_raw_path="/cache/raw.json",
        snippet="Stephan Weil ist ein deutscher Politiker.",
        snippet_source="lead_paragraph",
    )


def test_format_json(sample_resolved_evidence):
    """Test JSON formatting."""
    output = format_resolved_evidence_json([sample_resolved_evidence])
    
    assert output.startswith("[")
    data = json.loads(output)
    assert len(data) == 1
    assert data[0]["evidence_id"] == "test-evidence-123"
    assert data[0]["canonical_url"] == "https://de.wikipedia.org/w/index.php?title=Stephan_Weil&oldid=123456789"


def test_format_yaml(sample_resolved_evidence):
    """Test YAML formatting."""
    output = format_resolved_evidence_yaml([sample_resolved_evidence])
    
    assert "test-evidence-123" in output
    assert "Stephan_Weil" in output
    data = yaml.safe_load(output)
    assert len(data) == 1
    assert data[0]["evidence_id"] == "test-evidence-123"


def test_format_markdown(sample_resolved_evidence):
    """Test Markdown formatting."""
    output = format_resolved_evidence_markdown([sample_resolved_evidence])
    
    assert "Evidence `test-evidence-123`" in output
    assert "**URL**: https://de.wikipedia.org/w/index.php?title=Stephan_Weil&oldid=123456789" in output
    assert "**Snippet**: \"Stephan Weil ist ein deutscher Politiker.\"" in output
    assert "oldid=123456789" in output  # Canonical URL with oldid

