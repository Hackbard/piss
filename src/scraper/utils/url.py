from typing import Any, Dict, Optional
from urllib.parse import quote, urlencode

from scraper.config import get_settings

settings = get_settings()


def build_wikipedia_canonical_url(page_title: str, revision_id: Optional[int] = None) -> str:
    """
    Build canonical Wikipedia URL with oldid parameter for reproducibility.
    
    Format: https://de.wikipedia.org/w/index.php?title=<URLENCODED_TITLE>&oldid=<REVISION_ID>
    Fallback (no revision_id): https://de.wikipedia.org/wiki/<TITLE>
    """
    base_url = "https://de.wikipedia.org"
    title_encoded = quote(page_title.replace("_", " "), safe="")
    
    if revision_id:
        return f"{base_url}/w/index.php?title={title_encoded}&oldid={revision_id}"
    else:
        return f"{base_url}/wiki/{title_encoded}"


def build_dip_canonical_url(endpoint: str, params: Optional[Dict[str, Any]] = None) -> str:
    """
    Build canonical DIP URL from endpoint and params.
    
    Uses DIP_BASE_URL from settings.
    """
    base_url = settings.dip_base_url.rstrip("/")
    endpoint_clean = endpoint.lstrip("/")
    url = f"{base_url}/{endpoint_clean}"
    
    if params:
        query_string = urlencode(params, doseq=True)
        url = f"{url}?{query_string}"
    
    return url


def normalize_url(url: str) -> str:
    """
    Normalize URL: ensure proper encoding, no spaces.
    """
    if not url:
        return ""
    
    # If already a full URL, try to parse and rebuild
    if url.startswith("http"):
        from urllib.parse import urlparse, urlunparse, parse_qs, urlencode
        
        parsed = urlparse(url)
        query_params = parse_qs(parsed.query)
        normalized_query = urlencode(query_params, doseq=True)
        
        path_encoded = quote(parsed.path, safe="/")
        
        return urlunparse((
            parsed.scheme,
            parsed.netloc,
            path_encoded,
            parsed.params,
            normalized_query,
            parsed.fragment
        ))
    
    return quote(url, safe="/:?#[]@!$&'()*+,;=")

