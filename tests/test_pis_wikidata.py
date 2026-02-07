from datetime import datetime, timezone

from pis.ids import pis_person_id_from_wikidata_qid
from pis.ingest.wikidata import _parse_bindings
from pis.normalize.wikidata import wikidata_row_to_person


def test_parse_bindings_extracts_qid_and_title():
    payload = {
        "results": {
            "bindings": [
                {
                    "person": {"type": "uri", "value": "http://www.wikidata.org/entity/Q123"},
                    "personLabel": {"type": "literal", "value": "Test Person"},
                    "birth": {"type": "literal", "value": "1970-01-02T00:00:00Z"},
                    "dewikiTitle": {"type": "literal", "value": "Test Person"},
                }
            ]
        }
    }
    rows = _parse_bindings(payload)
    assert len(rows) == 1
    assert rows[0].qid == "Q123"
    assert rows[0].label == "Test Person"
    assert rows[0].birth_date == "1970-01-02"
    assert rows[0].dewiki_title == "Test_Person"


def test_wikidata_row_maps_to_canonical_person_with_deterministic_id():
    payload = {
        "results": {
            "bindings": [
                {
                    "person": {"type": "uri", "value": "http://www.wikidata.org/entity/Q999"},
                    "personLabel": {"type": "literal", "value": "Jane Doe"},
                }
            ]
        }
    }
    row = _parse_bindings(payload)[0]
    fetched_at = datetime(2026, 2, 7, tzinfo=timezone.utc)
    person = wikidata_row_to_person(
        row=row,
        fetched_at=fetched_at,
        raw_snapshot_path="/tmp/raw.json",
        normalized_snapshot_path="/tmp/norm.jsonl",
        source_url="https://query.wikidata.org/sparql",
    )
    assert person.external_ids.wikidata_qid == "Q999"
    assert person.pis_person_id == pis_person_id_from_wikidata_qid("Q999")
    assert person.sources and person.sources[0].source_person_id == "Q999"

