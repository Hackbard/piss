import pytest

from scraper.evidence.snippets import clean_snippet_text, extract_lead_paragraph


def test_clean_snippet_text_removes_footnotes():
    """Test that footnote markers are removed."""
    text = "This is a sentence[1] with footnotes[2]."
    cleaned = clean_snippet_text(text)
    
    assert "[1]" not in cleaned
    assert "[2]" not in cleaned
    assert "This is a sentence with footnotes." in cleaned


def test_clean_snippet_text_collapses_whitespace():
    """Test that whitespace is collapsed."""
    text = "This   has    multiple    spaces"
    cleaned = clean_snippet_text(text)
    
    assert "  " not in cleaned
    assert cleaned == "This has multiple spaces"


def test_extract_lead_paragraph():
    """Test extracting lead paragraph from HTML."""
    html = '''
    <div class="mw-parser-output">
        <p>This is the first paragraph with sufficient content to be selected as the lead paragraph.</p>
        <p>This is a shorter paragraph.</p>
    </div>
    '''
    
    snippet = extract_lead_paragraph(html, max_len=500)
    
    assert snippet is not None
    assert "first paragraph" in snippet
    assert "[" not in snippet  # No footnote markers


def test_extract_lead_paragraph_truncates():
    """Test that lead paragraph is truncated to max_len."""
    long_text = "A" * 1000
    html = f'<div class="mw-parser-output"><p>{long_text}</p></div>'
    
    snippet = extract_lead_paragraph(html, max_len=100)
    
    assert snippet is not None
    assert len(snippet) <= 100 + 3  # +3 for "..."
    assert snippet.endswith("...")


def test_extract_lead_paragraph_no_content():
    """Test that None is returned when no content."""
    html = '<div class="mw-parser-output"><p></p></div>'
    
    snippet = extract_lead_paragraph(html)
    
    assert snippet is None

