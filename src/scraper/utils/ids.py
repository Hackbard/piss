import uuid
from typing import Literal

NAMESPACE_PERSON = uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")
NAMESPACE_LEGISLATURE = uuid.UUID("6ba7b811-9dad-11d1-80b4-00c04fd430c8")
NAMESPACE_PARTY = uuid.UUID("6ba7b812-9dad-11d1-80b4-00c04fd430c8")
NAMESPACE_MANDATE = uuid.UUID("6ba7b813-9dad-11d1-80b4-00c04fd430c8")
NAMESPACE_EVIDENCE = uuid.UUID("6ba7b814-9dad-11d1-80b4-00c04fd430c8")


def generate_person_id(wikipedia_title: str) -> str:
    return str(uuid.uuid5(NAMESPACE_PERSON, wikipedia_title.lower().strip()))


def generate_legislature_id(parliament: str, state: str, number: int) -> str:
    key = f"{parliament}|{state}|{number}"
    return str(uuid.uuid5(NAMESPACE_LEGISLATURE, key))


def generate_party_id(party_name: str) -> str:
    return str(uuid.uuid5(NAMESPACE_PARTY, party_name.strip().lower()))


def generate_mandate_id(person_id: str, legislature_id: str, start: str, end: str, role: str = "") -> str:
    key = f"{person_id}|{legislature_id}|{start}|{end}|{role}"
    return str(uuid.uuid5(NAMESPACE_MANDATE, key))


def generate_evidence_id(page_id: int, revision_id: int, endpoint_kind: str, sha256: str) -> str:
    key = f"{page_id}|{revision_id}|{endpoint_kind}|{sha256}"
    return str(uuid.uuid5(NAMESPACE_EVIDENCE, key))

