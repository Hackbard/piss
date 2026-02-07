from __future__ import annotations

import uuid

NAMESPACE_PIS_PERSON = uuid.UUID("1b2d9f9e-3c3d-4f52-8b3a-5d0a6b7b0a01")
NAMESPACE_PIS_MEMBERSHIP = uuid.UUID("2d7b3b31-7a1a-4b1e-8f26-4d2d48dd2f02")
NAMESPACE_PIS_OFFICE_ROLE = uuid.UUID("7d4d0a0f-1d0b-4e7a-9e2e-5b12d7f6a003")
NAMESPACE_PIS_LEGISLATURE_PERIOD = uuid.UUID("a1c8d8b1-51f7-4c41-9ab1-0a5f64c1a004")


def uuid5_str(namespace: uuid.UUID, key: str) -> str:
    return str(uuid.uuid5(namespace, key))


def pis_person_id_from_wikidata_qid(qid: str) -> str:
    qid_norm = qid.strip()
    return uuid5_str(NAMESPACE_PIS_PERSON, f"wikidata:{qid_norm}")


def pis_person_id_from_dip_person_id(dip_person_id: int) -> str:
    return uuid5_str(NAMESPACE_PIS_PERSON, f"dip:{int(dip_person_id)}")


def pis_membership_id(pis_person_id: str, parliament_code: str, term_key: str, start: str, end: str) -> str:
    key = f"{pis_person_id}|{parliament_code.strip().upper()}|{term_key.strip()}|{start}|{end}"
    return uuid5_str(NAMESPACE_PIS_MEMBERSHIP, key)


def pis_office_role_id(pis_person_id: str, office_title: str, start: str, end: str) -> str:
    key = f"{pis_person_id}|{office_title.strip()}|{start}|{end}"
    return uuid5_str(NAMESPACE_PIS_OFFICE_ROLE, key)


def pis_legislature_period_id(parliament_code: str, term_number: int | None, name: str) -> str:
    key = f"{parliament_code.strip().upper()}|{term_number or ''}|{name.strip()}"
    return uuid5_str(NAMESPACE_PIS_LEGISLATURE_PERIOD, key)

