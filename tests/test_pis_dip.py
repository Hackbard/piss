from pis.ingest.dip import parse_person_page


def test_parse_dip_person_page_fixture():
    data = {
        "numFound": 250,
        "cursor": "cursor_page2",
        "documents": [
            {
                "id": 11000001,
                "vorname": "Max",
                "nachname": "Mustermann",
                "namenszusatz": None,
                "titel": "Dr.",
                "fraktion": "CDU/CSU",
                "wahlperiode": [19],
                "person_roles": [],
            }
        ],
    }
    page = parse_person_page(data)
    assert page.num_found == 250
    assert page.cursor == "cursor_page2"
    assert len(page.persons) == 1
    assert page.persons[0].dip_person_id == 11000001

