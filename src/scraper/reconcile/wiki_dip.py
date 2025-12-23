import re
import unicodedata
from typing import Any, Dict, List, Optional, Tuple

from scraper.models.domain import (
    CanonicalPerson,
    DipPersonRecord,
    PersonLinkAssertion,
    WikipediaPersonRecord,
)
from scraper.utils.ids import NAMESPACE_PERSON
from scraper.utils.time import utc_now_iso

RULESET_VERSION = "ruleset_v1"


def normalize_name(name: str) -> str:
    if not name:
        return ""
    name = name.lower().strip()
    name = re.sub(r"\s+", " ", name)
    name = unicodedata.normalize("NFKD", name)
    name = re.sub(r"[^\w\s]", "", name)
    return name


def normalize_umlauts(text: str) -> str:
    replacements = {
        "ä": "ae",
        "ö": "oe",
        "ü": "ue",
        "ß": "ss",
        "Ä": "Ae",
        "Ö": "Oe",
        "Ü": "Ue",
    }
    for old, new in replacements.items():
        text = text.replace(old, new)
    return text


def extract_name_parts(name: str) -> Tuple[str, str, Optional[str]]:
    parts = name.split()
    if len(parts) >= 2:
        vorname = parts[0]
        nachname = parts[-1]
        namenszusatz = " ".join(parts[1:-1]) if len(parts) > 2 else None
        return vorname, nachname, namenszusatz
    elif len(parts) == 1:
        return parts[0], "", None
    return "", "", None


def score_match(
    wiki_record: WikipediaPersonRecord,
    dip_record: DipPersonRecord,
) -> float:
    wiki_name_norm = normalize_name(wiki_record.name)
    wiki_vorname, wiki_nachname, wiki_namenszusatz = extract_name_parts(wiki_name_norm)

    dip_vorname_norm = normalize_name(dip_record.vorname or "")
    dip_nachname_norm = normalize_name(dip_record.nachname or "")
    dip_namenszusatz_norm = normalize_name(dip_record.namenszusatz or "")

    score = 0.0

    if wiki_nachname and dip_nachname_norm:
        if wiki_nachname == dip_nachname_norm:
            score += 0.5
        elif normalize_umlauts(wiki_nachname) == normalize_umlauts(dip_nachname_norm):
            score += 0.48

    if wiki_vorname and dip_vorname_norm:
        if wiki_vorname == dip_vorname_norm:
            score += 0.45
        elif normalize_umlauts(wiki_vorname) == normalize_umlauts(dip_vorname_norm):
            score += 0.43

    if wiki_namenszusatz and dip_namenszusatz_norm:
        if wiki_namenszusatz == dip_namenszusatz_norm:
            score += 0.05

    if score >= 0.95:
        return min(1.0, score)

    return score


def generate_canonical_id(wikipedia_title: Optional[str], dip_person_id: Optional[int]) -> str:
    import uuid
    if wikipedia_title:
        return str(uuid.uuid5(NAMESPACE_PERSON, f"wikipedia:{wikipedia_title.lower()}"))
    elif dip_person_id:
        return str(uuid.uuid5(NAMESPACE_PERSON, f"dip:{dip_person_id}"))
    else:
        return str(uuid.uuid5(NAMESPACE_PERSON, f"unknown:{utc_now_iso()}"))


def generate_link_assertion_id(
    wikipedia_ref: str, dip_ref: str, ruleset_version: str
) -> str:
    import uuid
    key = f"{wikipedia_ref}|{dip_ref}|{ruleset_version}"
    return str(uuid.uuid5(NAMESPACE_PERSON, key))


def load_link_overrides() -> Dict[str, Dict[str, Any]]:
    from pathlib import Path
    import yaml

    overrides_path = Path("config/link_overrides.yaml")
    if not overrides_path.exists():
        return {}

    with open(overrides_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
        return data.get("overrides", {})


def reconcile_wiki_dip(
    wiki_records: List[WikipediaPersonRecord],
    dip_records: List[DipPersonRecord],
    use_overrides: bool = True,
) -> Tuple[List[CanonicalPerson], List[PersonLinkAssertion]]:
    overrides = load_link_overrides() if use_overrides else {}
    canonical_persons: List[CanonicalPerson] = []
    assertions: List[PersonLinkAssertion] = []

    for wiki_record in wiki_records:
        override_key = wiki_record.wikipedia_title
        override = overrides.get(override_key) if overrides else None

        if override and isinstance(override, dict) and override.get("dip_person_id"):
            dip_id = str(override["dip_person_id"])
            dip_record = next((d for d in dip_records if str(d.dip_person_id) == dip_id), None)

            if dip_record:
                assertion = PersonLinkAssertion(
                    id=generate_link_assertion_id(
                        wiki_record.id, dip_id, RULESET_VERSION
                    ),
                    wikipedia_person_ref=wiki_record.id,
                    dip_person_ref=dip_id,
                    ruleset_version=RULESET_VERSION,
                    method="override",
                    score=1.0,
                    status="accepted" if override.get("status") != "rejected" else "rejected",
                    reason=override.get("reason", "Manual override"),
                    evidence_ids=wiki_record.evidence_ids + dip_record.evidence_ids,
                    created_at=utc_now_iso(),
                )
                assertions.append(assertion)

                if assertion.status == "accepted":
                    canonical_id = generate_canonical_id(
                        wiki_record.wikipedia_title, dip_record.dip_person_id
                    )
                    canonical = CanonicalPerson(
                        id=canonical_id,
                        display_name=wiki_record.name,
                        identifiers={
                            "wikipedia_title": wiki_record.wikipedia_title,
                            "wikipedia_page_id": str(wiki_record.page_id),
                            "dip_person_id": str(dip_record.dip_person_id),
                        },
                        created_at=utc_now_iso(),
                        updated_at=utc_now_iso(),
                        evidence_ids=wiki_record.evidence_ids + dip_record.evidence_ids,
                    )
                    canonical_persons.append(canonical)
            continue

        candidates: List[Tuple[DipPersonRecord, float]] = []
        for dip_record in dip_records:
            score = score_match(wiki_record, dip_record)
            if score >= 0.5:
                candidates.append((dip_record, score))

        candidates.sort(key=lambda x: x[1], reverse=True)

        if not candidates:
            assertion = PersonLinkAssertion(
                id=generate_link_assertion_id(
                    wiki_record.id, "none", RULESET_VERSION
                ),
                wikipedia_person_ref=wiki_record.id,
                dip_person_ref="none",
                ruleset_version=RULESET_VERSION,
                method="ruleset",
                score=0.0,
                status="pending",
                reason="No candidate found with score >= 0.5",
                evidence_ids=wiki_record.evidence_ids,
                created_at=utc_now_iso(),
            )
            assertions.append(assertion)
            continue

        best_score = candidates[0][1]
        second_best_score = candidates[1][1] if len(candidates) > 1 else 0.0

        if best_score >= 0.95 and (best_score - second_best_score) >= 0.05:
            dip_record = candidates[0][0]
            assertion = PersonLinkAssertion(
                id=generate_link_assertion_id(
                    wiki_record.id, str(dip_record.dip_person_id), RULESET_VERSION
                ),
                wikipedia_person_ref=wiki_record.id,
                dip_person_ref=str(dip_record.dip_person_id),
                ruleset_version=RULESET_VERSION,
                method="ruleset",
                score=best_score,
                status="accepted",
                reason=f"Unique match with score {best_score:.2f}",
                evidence_ids=wiki_record.evidence_ids + dip_record.evidence_ids,
                created_at=utc_now_iso(),
            )
            assertions.append(assertion)

            canonical_id = generate_canonical_id(
                wiki_record.wikipedia_title, dip_record.dip_person_id
            )
            canonical = CanonicalPerson(
                id=canonical_id,
                display_name=wiki_record.name,
                identifiers={
                    "wikipedia_title": wiki_record.wikipedia_title,
                    "wikipedia_page_id": str(wiki_record.page_id),
                    "dip_person_id": str(dip_record.dip_person_id),
                },
                created_at=utc_now_iso(),
                updated_at=utc_now_iso(),
                evidence_ids=wiki_record.evidence_ids + dip_record.evidence_ids,
            )
            canonical_persons.append(canonical)
        else:
            for dip_record, score in candidates[:3]:
                assertion = PersonLinkAssertion(
                    id=generate_link_assertion_id(
                        wiki_record.id, str(dip_record.dip_person_id), RULESET_VERSION
                    ),
                    wikipedia_person_ref=wiki_record.id,
                    dip_person_ref=str(dip_record.dip_person_id),
                    ruleset_version=RULESET_VERSION,
                    method="ruleset",
                    score=score,
                    status="pending",
                    reason=f"Ambiguous match: best={best_score:.2f}, second={second_best_score:.2f}",
                    evidence_ids=wiki_record.evidence_ids + dip_record.evidence_ids,
                    created_at=utc_now_iso(),
                )
                assertions.append(assertion)

    return canonical_persons, assertions

