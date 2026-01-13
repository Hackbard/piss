from langgraph_app.sources.wikidata_terms import parse_term_from_entitydata


def test_wikidata_day_precision_only_sets_iso_day():
    entitydata = {
        "entities": {
            "Q1": {
                "claims": {
                    "P580": [
                        {
                            "mainsnak": {
                                "datavalue": {
                                    "value": {"time": "+2017-10-15T00:00:00Z", "precision": 11}
                                }
                            }
                        }
                    ],
                    "P582": [
                        {
                            "mainsnak": {
                                "datavalue": {"value": {"time": "+2022-11-08T00:00:00Z", "precision": 11}}
                            }
                        }
                    ],
                },
                "labels": {"de": {"language": "de", "value": "Testterm"}},
            }
        }
    }

    term = parse_term_from_entitydata("Q1", 123, entitydata)
    assert term.start.value_iso == "2017-10-15"
    assert term.end.value_iso == "2022-11-08"


def test_wikidata_month_precision_does_not_set_iso_day():
    entitydata = {
        "entities": {
            "Q2": {
                "claims": {
                    "P580": [
                        {
                            "mainsnak": {
                                "datavalue": {
                                    "value": {"time": "+2017-10-00T00:00:00Z", "precision": 10}
                                }
                            }
                        }
                    ]
                }
            }
        }
    }

    term = parse_term_from_entitydata("Q2", 456, entitydata)
    assert term.start.value_iso is None
    assert term.start.precision == 10
    assert term.start.raw == "+2017-10-00T00:00:00Z"

