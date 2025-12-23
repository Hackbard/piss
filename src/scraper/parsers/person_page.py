import re
from typing import Any, Dict, List, Optional

from bs4 import BeautifulSoup
from dateutil.parser import parse as parse_date

from scraper.mediawiki.types import MediaWikiParseResponse
from scraper.models.domain import Person
from scraper.utils.ids import generate_evidence_id, generate_person_id
from scraper.utils.hashing import sha256_hash_json


def extract_intro(soup: BeautifulSoup) -> str:
    content = soup.find("div", class_="mw-parser-output")
    if not content:
        return ""

    paragraphs = []
    for elem in content.children:
        if hasattr(elem, "name"):
            if elem.name == "p" and elem.get_text().strip():
                text = elem.get_text().strip()
                if not text.startswith("Koordinaten"):
                    paragraphs.append(text)
            elif elem.name in ["h2", "h3", "table"]:
                break

    return "\n\n".join(paragraphs)


def extract_infobox_keyfacts(soup: BeautifulSoup) -> Dict[str, Any]:
    infobox = soup.find("table", class_=re.compile(r"infobox|biografie"))
    if not infobox:
        return {}

    keyfacts = {}
    rows = infobox.find_all("tr")
    for row in rows:
        th = row.find("th")
        td = row.find("td")
        if not th or not td:
            continue

        label = th.get_text().strip().lower()
        value = td.get_text().strip()

        if "geburt" in label or "geboren" in label:
            # Only extract from hard sources: <span class="bday"> or <time datetime="...">
            birth_date_extracted = False
            
            # Check for <span class="bday">YYYY-MM-DD</span>
            bday_span = td.find("span", class_="bday")
            if bday_span:
                date_str = bday_span.get_text().strip()
                try:
                    dt = parse_date(date_str, fuzzy=False)
                    keyfacts["birth_date"] = dt.date().isoformat()
                    keyfacts["birth_date_status"] = "extracted"
                    birth_date_extracted = True
                except (ValueError, TypeError):
                    pass
            
            # Check for <time datetime="YYYY-MM-DD">...</time>
            if not birth_date_extracted:
                time_tag = td.find("time")
                if time_tag and time_tag.get("datetime"):
                    date_str = time_tag.get("datetime")
                    try:
                        dt = parse_date(date_str, fuzzy=False)
                        keyfacts["birth_date"] = dt.date().isoformat()
                        keyfacts["birth_date_status"] = "extracted"
                        birth_date_extracted = True
                    except (ValueError, TypeError):
                        pass
            
            if not birth_date_extracted:
                keyfacts["birth_date_status"] = "not_present"

        elif "tod" in label or "gestorben" in label or "verstorben" in label:
            time_tag = td.find("time")
            if time_tag:
                date_str = time_tag.get("datetime") or time_tag.get_text().strip()
            else:
                date_str = value
            try:
                dt = parse_date(date_str, fuzzy=True)
                keyfacts["death_date"] = dt.date().isoformat()
            except (ValueError, TypeError):
                pass

    return keyfacts


def parse_person_page(response: MediaWikiParseResponse) -> Person:
    soup = BeautifulSoup(response.html, "html.parser")

    title = response.page_title.replace("_", " ")
    intro = extract_intro(soup)
    keyfacts = extract_infobox_keyfacts(soup)

    # Use sha256 from metadata if available, otherwise compute from parse
    # This ensures consistency with the evidence index
    from scraper.cache.mediawiki_cache import get_cached_metadata
    metadata = get_cached_metadata(response.page_title)
    sha256 = metadata.sha256 if metadata and metadata.sha256 else sha256_hash_json(response.parse)
    
    evidence_id = generate_evidence_id(
        response.page_id,
        response.revision_id,
        "parse",
        sha256,
    )

    person_id = generate_person_id(response.page_title)
    wikipedia_url = f"https://de.wikipedia.org/wiki/{response.page_title}"

    unstructured_evidence = []
    if intro:
        unstructured_evidence.append(
            {
                "type": "intro",
                "text": intro[:500],
                "evidence_id": evidence_id,
            }
        )

    birth_date = keyfacts.get("birth_date")
    birth_date_status = keyfacts.get("birth_date_status", "unknown")
    data_quality_flags = []
    if not birth_date:
        data_quality_flags.append("missing_birth_date")
    
    # Create EvidenceRef for person page intro
    from scraper.models.domain import EvidenceRef
    from scraper.utils.time import utc_now_iso
    
    person_page_evidence_ref = EvidenceRef(
        evidence_id=evidence_id,
        snippet_ref=None,  # Lead paragraph, no specific row reference
        purpose="person_page_intro",
        created_at=utc_now_iso(),
    )
    
    return Person(
        id=person_id,
        name=title,
        wikipedia_title=response.page_title,
        wikipedia_url=wikipedia_url,
        birth_date=birth_date,
        birth_date_status=birth_date_status,
        death_date=keyfacts.get("death_date"),
        intro=intro,
        evidence_refs=[person_page_evidence_ref],  # Person page EvidenceRef
        evidence_ids=[evidence_id],  # Legacy: will be derived from evidence_refs
        unstructured_evidence=unstructured_evidence if unstructured_evidence else None,
        data_quality_flags=data_quality_flags,
    )

