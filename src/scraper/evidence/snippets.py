import re
from typing import Any, Dict, Optional

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


def extract_table_row_snippet(html: str, snippet_ref: Dict[str, Any], max_len: int = 500) -> Optional[str]:
    """
    Extract snippet from specific table row based on snippet_ref dict.
    
    snippet_ref format:
    {
        "version": 1,
        "type": "table_row",
        "table_index": <int>,
        "row_index": <int>,
        "row_kind": "data",
        ...
    }
    """
    if not html or not snippet_ref:
        return None
    
    if snippet_ref.get("type") != "table_row":
        return None
    
    table_index = snippet_ref.get("table_index", 0)
    row_index = snippet_ref.get("row_index", 0)
    
    soup = BeautifulSoup(html, "html.parser")
    
    # Find all wikitable tables (or all tables if none)
    all_tables = soup.find_all("table", class_=lambda x: x and "wikitable" in x)
    if not all_tables:
        all_tables = soup.find_all("table")
    
    if table_index >= len(all_tables):
        return None
    
    table = all_tables[table_index]
    rows = table.find_all("tr")
    
    # Skip header row, row_index is 0-based for data rows
    data_rows = rows[1:] if len(rows) > 1 else rows
    
    if row_index >= len(data_rows):
        return None
    
    row = data_rows[row_index]
    cells = row.find_all(["td", "th"])
    
    # Extract text from cells and join with " | "
    cell_texts = []
    for cell in cells:
        text = cell.get_text().strip()
        if text:
            cell_texts.append(text)
    
    if not cell_texts:
        return None
    
    # Join cells with " | " separator
    snippet = " | ".join(cell_texts)
    cleaned = clean_snippet_text(snippet)
    
    if cleaned:
        if len(cleaned) > max_len:
            cleaned = cleaned[:max_len].rsplit(' ', 1)[0] + "..."
        return cleaned
    
    return None


def extract_snippet(
    html: Optional[str],
    snippet_ref: Optional[Any] = None,
    max_len: int = 500,
    prefer: str = "table_row"
) -> tuple[Optional[str], Optional[str]]:
    """
    Extract snippet from HTML.
    
    Args:
        html: HTML content
        snippet_ref: Dict with snippet_ref structure or legacy string format
        max_len: Maximum snippet length
        prefer: Preferred snippet type ("table_row" or "lead_paragraph")
    
    Returns: (snippet, snippet_source)
    """
    if not html:
        return None, None
    
    # Handle snippet_ref (can be Dict or legacy string)
    if snippet_ref:
        if isinstance(snippet_ref, dict) and snippet_ref.get("type") == "table_row":
            snippet = extract_table_row_snippet(html, snippet_ref, max_len)
            if snippet:
                return snippet, "table_row"
        elif isinstance(snippet_ref, str) and snippet_ref.startswith("table_row:"):
            # Legacy format support
            try:
                parts = snippet_ref.split(":")
                ref_dict = {
                    "type": "table_row",
                    "table_index": int(parts[1]) if len(parts) > 1 else 0,
                    "row_index": int(parts[2]) if len(parts) > 2 else 0,
                }
                snippet = extract_table_row_snippet(html, ref_dict, max_len)
                if snippet:
                    return snippet, "table_row"
            except (ValueError, IndexError):
                pass
    
    # Fallback: lead paragraph (if prefer is lead_paragraph or table_row not available)
    if prefer == "lead_paragraph" or (prefer == "table_row" and not snippet_ref):
        snippet = extract_lead_paragraph(html, max_len)
        if snippet:
            return snippet, "lead_paragraph"
    
    return None, None

