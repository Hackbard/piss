import uuid
from typing import Literal, Optional

NAMESPACE_PERSON = uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")
NAMESPACE_LEGISLATURE = uuid.UUID("6ba7b811-9dad-11d1-80b4-00c04fd430c8")
NAMESPACE_PARTY = uuid.UUID("6ba7b812-9dad-11d1-80b4-00c04fd430c8")
NAMESPACE_MANDATE = uuid.UUID("6ba7b813-9dad-11d1-80b4-00c04fd430c8")
NAMESPACE_EVIDENCE = uuid.UUID("6ba7b814-9dad-11d1-80b4-00c04fd430c8")
NAMESPACE_PARLIAMENT = uuid.UUID("6ba7b815-9dad-11d1-80b4-00c04fd430c8")


def generate_person_id(wikipedia_title: str) -> str:
    return str(uuid.uuid5(NAMESPACE_PERSON, wikipedia_title.lower().strip()))


def generate_parliament_id(name: str, level: str, state_code: Optional[str] = None) -> str:
    key = f"{name}|{level}|{state_code or ''}"
    return str(uuid.uuid5(NAMESPACE_PARLIAMENT, key))


def generate_legislature_id(parliament_id_code: str, legislature_number: int) -> str:
    key = f"{parliament_id_code}|{legislature_number}"
    return str(uuid.uuid5(NAMESPACE_LEGISLATURE, key))


def generate_party_id(party_code: str) -> str:
    return str(uuid.uuid5(NAMESPACE_PARTY, party_code.strip().upper()))


def generate_mandate_id(person_id: str, legislature_id: str, start: str, end: str, role: str = "", party_code: Optional[str] = None) -> str:
    normalized_end = end if end and end != "unknown" else ""
    normalized_party = (party_code or "").strip().upper()
    key = f"{person_id}|{legislature_id}|{start}|{normalized_end}|{role}|{normalized_party}"
    return str(uuid.uuid5(NAMESPACE_MANDATE, key))


def generate_evidence_id(page_id: int, revision_id: int, endpoint_kind: str, sha256: str) -> str:
    key = f"{page_id}|{revision_id}|{endpoint_kind}|{sha256}"
    return str(uuid.uuid5(NAMESPACE_EVIDENCE, key))

