from pis.rag.retrieve import _compact_hit


def test_compact_hit_extracts_source_urls_and_ids():
    hit = {
        "pis_person_id": "pid",
        "display_name": "X",
        "external_ids": {"wikidata_qid": "Q1", "dip_person_id": 1},
        "provenance": {"sources": [{"source_urls": ["https://a", "https://b"]}]},
        "facts": {"k": "v"},
    }
    out = _compact_hit(hit)
    assert out["pis_person_id"] == "pid"
    assert out["external_ids"]["wikidata_qid"] == "Q1"
    assert out["source_urls"] == ["https://a", "https://b"]

