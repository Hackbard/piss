import re
from datetime import datetime
from typing import Any, Dict, List, Optional

from bs4 import BeautifulSoup
from dateutil.parser import parse as parse_date

from scraper.mediawiki.types import MediaWikiParseResponse
from scraper.models.domain import Event, LegislatureMember, Mandate, Person
from scraper.utils.ids import generate_evidence_id, generate_mandate_id, generate_person_id
from scraper.utils.hashing import sha256_hash_json
from scraper.utils.time import utc_now_iso


def normalize_header(text: str) -> str:
    return re.sub(r"\s+", " ", text.strip().lower())


def find_members_table(soup: BeautifulSoup, seed_hints: Optional[Dict[str, Any]] = None) -> Optional[Any]:
    keywords = ["mitglieder", "abgeordnete", "fraktionen", "fraktion", "partei", "wahlkreis", "anmerkungen"]
    if seed_hints and "section_keywords" in seed_hints:
        keywords.extend(seed_hints["section_keywords"])

    all_tables = soup.find_all("table")
    
    for table in all_tables:
        header_row = table.find("tr")
        if not header_row:
            continue
        
        headers = header_row.find_all(["th", "td"])
        header_texts = [normalize_header(h.get_text()) for h in headers]
        
        has_name = any("name" in ht for ht in header_texts)
        has_party_or_fraktion = any("partei" in ht or "fraktion" in ht for ht in header_texts)
        has_wahlkreis = any("wahlkreis" in ht for ht in header_texts)
        
        if has_name and (has_party_or_fraktion or has_wahlkreis):
            return table

    for heading in soup.find_all(["h2", "h3", "h4"]):
        heading_text = normalize_header(heading.get_text())
        for keyword in keywords:
            if keyword in heading_text:
                next_table = heading.find_next_sibling("table")
                if next_table:
                    return next_table
                for sibling in heading.next_siblings:
                    if hasattr(sibling, "name") and sibling.name == "table":
                        return sibling

    return None


def extract_table_headers(table: Any) -> Dict[str, int]:
    headers = {}
    header_row = table.find("tr")
    if not header_row:
        return headers

    cells = header_row.find_all(["th", "td"])
    for idx, cell in enumerate(cells):
        text = normalize_header(cell.get_text())
        if "name" in text or "abgeordnete" in text:
            headers["name"] = idx
        elif "partei" in text or "fraktion" in text:
            headers["party"] = idx
        elif "wahlkreis" in text:
            headers["wahlkreis"] = idx
        elif "anmerkung" in text or "bemerkung" in text or "notiz" in text:
            headers["notes"] = idx
        elif "von" in text or "start" in text or "beginn" in text:
            headers["start"] = idx
        elif "bis" in text or "ende" in text:
            headers["end"] = idx

    return headers


def parse_event_from_notes(notes_text: str, evidence_id: str) -> List[Event]:
    events = []
    notes_lower = notes_text.lower()

    event_patterns = [
        (r"nachgerückt", "nachgerückt"),
        (r"ausgeschieden", "ausgeschieden"),
        (r"fraktionsaustritt", "fraktionsaustritt"),
        (r"parteiwechsel", "parteiwechsel"),
        (r"fraktionswechsel", "fraktionswechsel"),
    ]

    for pattern, event_type in event_patterns:
        if re.search(pattern, notes_lower):
            events.append(
                Event(
                    event_type=event_type,
                    description=notes_text.strip(),
                    evidence_ids=[evidence_id],
                )
            )

    return events


def parse_date_safe(date_str: Optional[str]) -> Optional[str]:
    if not date_str:
        return None
    try:
        dt = parse_date(date_str, fuzzy=True)
        return dt.date().isoformat()
    except (ValueError, TypeError):
        return None


def extract_person_from_row(
    row: Any,
    headers: Dict[str, int],
    seed_data: Dict[str, Any],
    evidence_id: str,
) -> Optional[Person]:
    cells = row.find_all(["td", "th"])
    if len(cells) <= max(headers.values(), default=0):
        return None

    name_cell_idx = headers.get("name")
    if name_cell_idx is None:
        return None

    name_cell = cells[name_cell_idx]
    name_link = name_cell.find("a")
    if not name_link:
        return None

    wikipedia_title = name_link.get("title", "").replace(" ", "_")
    if not wikipedia_title:
        name_text = name_cell.get_text().strip()
        wikipedia_title = name_text.replace(" ", "_")

    name = name_link.get_text().strip() or name_cell.get_text().strip()
    if not name:
        return None

    person_id = generate_person_id(wikipedia_title)
    wikipedia_url = f"https://de.wikipedia.org/wiki/{wikipedia_title}"

    return Person(
        id=person_id,
        name=name,
        wikipedia_title=wikipedia_title,
        wikipedia_url=wikipedia_url,
        evidence_ids=[evidence_id],
    )


def extract_mandate_from_row(
    row: Any,
    headers: Dict[str, int],
    person: Person,
    seed_data: Dict[str, Any],
    evidence_id: str,
) -> Optional[Mandate]:
    cells = row.find_all(["td", "th"])

    party_cell_idx = headers.get("party")
    party_name = None
    if party_cell_idx is not None and party_cell_idx < len(cells):
        party_name = cells[party_cell_idx].get_text().strip()

    wahlkreis = None
    wahlkreis_idx = headers.get("wahlkreis")
    if wahlkreis_idx is not None and wahlkreis_idx < len(cells):
        wahlkreis = cells[wahlkreis_idx].get_text().strip()

    notes = None
    notes_idx = headers.get("notes")
    if notes_idx is not None and notes_idx < len(cells):
        notes = cells[notes_idx].get_text().strip()

    time_range = seed_data.get("expected_time_range", {})
    start_date = parse_date_safe(time_range.get("start"))
    end_date = parse_date_safe(time_range.get("end"))

    start_idx = headers.get("start")
    if start_idx is not None and start_idx < len(cells):
        parsed_start = parse_date_safe(cells[start_idx].get_text().strip())
        if parsed_start:
            start_date = parsed_start

    end_idx = headers.get("end")
    if end_idx is not None and end_idx < len(cells):
        parsed_end = parse_date_safe(cells[end_idx].get_text().strip())
        if parsed_end:
            end_date = parsed_end

    if not start_date:
        start_date = time_range.get("start")
    if not end_date:
        end_date = time_range.get("end")

    events = []
    if notes:
        events = parse_event_from_notes(notes, evidence_id)

    legislature_id = None
    parliament = seed_data.get("hints", {}).get("parliament", "")
    state = seed_data.get("hints", {}).get("state", "")
    legislature_number = seed_data.get("hints", {}).get("legislature_number")
    if parliament and state and legislature_number:
        from scraper.utils.ids import generate_legislature_id

        legislature_id = generate_legislature_id(parliament, state, legislature_number)

    mandate_id = generate_mandate_id(
        person.id,
        legislature_id or "unknown",
        start_date or "unknown",
        end_date or "unknown",
        role="member",
    )

    return Mandate(
        id=mandate_id,
        person_id=person.id,
        legislature_id=legislature_id,
        party_name=party_name,
        wahlkreis=wahlkreis,
        start_date=start_date,
        end_date=end_date,
        role="member",
        events=events,
        notes=notes,
        evidence_ids=[evidence_id],
    )


def parse_legislature_members(
    response: MediaWikiParseResponse, seed_key: str
) -> LegislatureMember:
    from scraper.cache.mediawiki_cache import get_seed

    seed_data = get_seed(seed_key)
    soup = BeautifulSoup(response.html, "html.parser")

    table = find_members_table(soup, seed_data.get("hints"))
    if not table:
        raise ValueError("Could not find members table")

    headers = extract_table_headers(table)
    if not headers:
        raise ValueError("Could not extract table headers")

    evidence_id = generate_evidence_id(
        response.page_id,
        response.revision_id,
        "parse",
        sha256_hash_json(response.parse),
    )

    members = []
    rows = table.find_all("tr")[1:]
    for row in rows:
        person = extract_person_from_row(row, headers, seed_data, evidence_id)
        if not person:
            continue

        mandate = extract_mandate_from_row(row, headers, person, seed_data, evidence_id)
        if mandate:
            members.append((person, mandate))

    return LegislatureMember(
        seed_key=seed_key,
        page_title=response.page_title,
        page_id=response.page_id,
        revision_id=response.revision_id,
        members=members,
        evidence_id=evidence_id,
    )

