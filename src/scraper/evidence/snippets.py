import re
from typing import Optional

from bs4 import BeautifulSoup


def clean_snippet_text(text: str) -> str:
    """
    Clean snippet text:
    - Remove footnote markers like [1], [2]
    - Collapse whitespace
    - Strip
    """
    if not text:
        return ""
    
    # Remove footnote markers [1], [2], etc.
    text = re.sub(r'\[\d+\]', '', text)
    
    # Collapse whitespace
    text = re.sub(r'\s+', ' ', text)
    
    # Strip
    text = text.strip()
    
    return text


def extract_lead_paragraph(html: str, max_len: int = 500) -> Optional[str]:
    """
    Extract first clean paragraph from MediaWiki parsed HTML.
    
    Looks for <p> tags in mw-parser-output, picks first with len >= 80 chars.
    """
    if not html:
        return None
    
    soup = BeautifulSoup(html, "html.parser")
    parser_output = soup.find("div", class_="mw-parser-output")
    
    if not parser_output:
        return None
    
    # Find first <p> with sufficient content
    for p in parser_output.find_all("p"):
        text = p.get_text()
        cleaned = clean_snippet_text(text)
        
        if len(cleaned) >= 80:
            if len(cleaned) > max_len:
                cleaned = cleaned[:max_len].rsplit(' ', 1)[0] + "..."
            return cleaned
    
    # Fallback: any <p> with content
    for p in parser_output.find_all("p"):
        text = p.get_text()
        cleaned = clean_snippet_text(text)
        
        if cleaned:
            if len(cleaned) > max_len:
                cleaned = cleaned[:max_len].rsplit(' ', 1)[0] + "..."
            return cleaned
    
    return None


def extract_table_row_snippet(html: str, snippet_ref: str, max_len: int = 500) -> Optional[str]:
    """
    Extract snippet from specific table row based on snippet_ref.
    
    snippet_ref format: "table_row:<table_index>:<row_index>"
    """
    if not html or not snippet_ref:
        return None
    
    if not snippet_ref.startswith("table_row:"):
        return None
    
    try:
        parts = snippet_ref.split(":")
        table_index = int(parts[1]) if len(parts) > 1 else 0
        row_index = int(parts[2]) if len(parts) > 2 else 0
    except (ValueError, IndexError):
        return None
    
    soup = BeautifulSoup(html, "html.parser")
    tables = soup.find_all("table")
    
    if table_index >= len(tables):
        return None
    
    table = tables[table_index]
    rows = table.find_all("tr")
    
    if row_index >= len(rows):
        return None
    
    row = rows[row_index]
    text = row.get_text()
    cleaned = clean_snippet_text(text)
    
    if cleaned:
        if len(cleaned) > max_len:
            cleaned = cleaned[:max_len].rsplit(' ', 1)[0] + "..."
        return cleaned
    
    return None


def extract_snippet(
    html: Optional[str],
    snippet_ref: Optional[str] = None,
    max_len: int = 500
) -> tuple[Optional[str], Optional[str]]:
    """
    Extract snippet from HTML.
    
    Returns: (snippet, snippet_source)
    """
    if not html:
        return None, None
    
    # If snippet_ref points to table row, use that
    if snippet_ref and snippet_ref.startswith("table_row:"):
        snippet = extract_table_row_snippet(html, snippet_ref, max_len)
        if snippet:
            return snippet, "table_row"
    
    # Default: lead paragraph
    snippet = extract_lead_paragraph(html, max_len)
    if snippet:
        return snippet, "lead_paragraph"
    
    return None, None

