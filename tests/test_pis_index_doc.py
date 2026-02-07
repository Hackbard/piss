from datetime import datetime, timezone

from pis.models import ExternalPersonIds, Person, PersonSource, SourceSystem
from pis.normalize.wikidata import person_to_index_doc


def test_person_to_index_doc_is_json_serializable_shape():
    now = datetime(2026, 2, 7, tzinfo=timezone.utc)
    p = Person(
        pis_person_id="pid",
        display_name="Test",
        created_at=now,
        updated_at=now,
        external_ids=ExternalPersonIds(wikidata_qid="Q1", wikipedia_title="X"),
        sources=[
            PersonSource(
                source_system=SourceSystem.WIKIDATA,
                source_person_id="Q1",
                fetched_at=now,
                source_urls=["https://example.com"],
            )
        ],
    )
    doc = person_to_index_doc(p)
    assert doc["pis_person_id"] == "pid"
    assert doc["external_ids"]["wikidata_qid"] == "Q1"
    assert doc["provenance"]["sources"][0]["source_system"] == "wikidata"

