"""Tests for Policy Guard Node."""

from datetime import date

from langgraph_app.config import OrchestratorConfig, PolicyMode
from langgraph_app.nodes.policy_guard import PolicyDecision, policy_guard_node
from langgraph_app.schemas import ComputedResult, IntentType, UserIntent


def test_policy_guard_refuses_ranking():
    """Test policy guard blocks ranking questions."""
    config = OrchestratorConfig(policy_mode=PolicyMode.NEUTRAL_STRICT)
    state = {
        "question": "Wer ist die schlimmste Partei?",
        "intent": UserIntent(
            intent_type=IntentType.LEGISLATURE_STATS,
            strict_evidence=True,
        ),
        "tool_results": [
            {
                "tool_name": "legislature.stats",
                "data": {
                    "total_seats": 100,
                    "party_seats": {"SPD": 40, "CDU": 60},
                    "evidence_urls": ["https://example.com/evidence1"],
                },
            }
        ],
        "computed": ComputedResult(
            computed_metrics={"seat_share_percent": {"SPD": 40.0, "CDU": 60.0}}
        ).model_dump(),
    }

    result = policy_guard_node(state, config)

    assert result["policy_decision"] == PolicyDecision.REFUSE_RANKING
    assert "ranking" in result["policy_warnings"][0].lower()
    assert "alternative" in result["safe_answer_plan"]


def test_policy_guard_enforces_sources():
    """Test policy guard enforces sources requirement."""
    config = OrchestratorConfig(policy_mode=PolicyMode.NEUTRAL_STRICT)
    state = {
        "question": "Alle SPD-Mitglieder im Landtag Niedersachsen",
        "intent": UserIntent(
            intent_type=IntentType.MANDATES_LIST,
            strict_evidence=True,
            parliament_id="nds",
        ),
        "tool_results": [
            {
                "tool_name": "mandates.search",
                "data": {
                    "rows": [
                        {
                            "mandate_id": "mandate-1",
                            "evidence_urls": [],
                        }
                    ]
                },
            }
        ],
        "computed": ComputedResult().model_dump(),
    }

    result = policy_guard_node(state, config)

    assert result["policy_decision"] == PolicyDecision.REWRITE_REQUIRED
    assert any("evidence" in w.lower() for w in result["policy_warnings"])


def test_policy_guard_passes_with_sources():
    """Test policy guard passes when sources are present."""
    config = OrchestratorConfig(policy_mode=PolicyMode.NEUTRAL_STRICT)
    state = {
        "question": "Alle SPD-Mitglieder im Landtag Niedersachsen",
        "intent": UserIntent(
            intent_type=IntentType.MANDATES_LIST,
            strict_evidence=True,
            parliament_id="nds",
        ),
        "tool_results": [
            {
                "tool_name": "mandates.search",
                "data": {
                    "rows": [
                        {
                            "mandate_id": "mandate-1",
                            "evidence_urls": ["https://example.com/evidence1"],
                        }
                    ]
                },
            }
        ],
        "computed": ComputedResult().model_dump(),
    }

    result = policy_guard_node(state, config)

    assert result["policy_decision"] == PolicyDecision.PASS
    assert len(result["safe_answer_plan"]["sources"]) > 0


def test_policy_guard_blocks_valuative_language():
    """Test policy guard blocks valuative language."""
    config = OrchestratorConfig(
        policy_mode=PolicyMode.NEUTRAL_STRICT,
        disallowed_phrases_strict="korrupt,skandalös",
    )
    state = {
        "question": "Die korrupte Partei X",
        "intent": UserIntent(
            intent_type=IntentType.LEGISLATURE_STATS,
            strict_evidence=True,
        ),
        "tool_results": [
            {
                "tool_name": "legislature.stats",
                "data": {
                    "total_seats": 100,
                    "party_seats": {"SPD": 40},
                    "evidence_urls": ["https://example.com/evidence1"],
                },
            }
        ],
        "computed": ComputedResult().model_dump(),
    }

    result = policy_guard_node(state, config)

    assert result["policy_decision"] == PolicyDecision.REWRITE_REQUIRED
    assert any("korrupt" in w.lower() or "blockierte" in w.lower() for w in result["policy_warnings"])


def test_policy_guard_needs_clarification():
    """Test policy guard handles needs_clarification."""
    config = OrchestratorConfig(policy_mode=PolicyMode.NEUTRAL_STRICT)
    state = {
        "question": "Welche Partei?",
        "intent": UserIntent(
            intent_type=IntentType.LEGISLATURE_STATS,
            needs_clarification=True,
            clarifying_question="Meinst du Landtag Niedersachsen oder Bundestag?",
            strict_evidence=True,
        ),
        "tool_results": [],
        "computed": None,
    }

    result = policy_guard_node(state, config)

    assert result["policy_decision"] == PolicyDecision.NEEDS_CLARIFICATION
    assert "clarifying_question" in result["safe_answer_plan"]


def test_policy_guard_checks_scope_clarity():
    """Test policy guard checks scope clarity."""
    config = OrchestratorConfig(policy_mode=PolicyMode.NEUTRAL_STRICT)
    state = {
        "question": "Alle Mandate",
        "intent": UserIntent(
            intent_type=IntentType.MANDATES_LIST,
            strict_evidence=True,
        ),
        "tool_results": [
            {
                "tool_name": "mandates.search",
                "data": {
                    "rows": [
                        {
                            "mandate_id": "mandate-1",
                            "evidence_urls": ["https://example.com/evidence1"],
                        }
                    ]
                },
            }
        ],
        "computed": ComputedResult().model_dump(),
    }

    result = policy_guard_node(state, config)

    assert any("parlament" in w.lower() or "zeitraum" in w.lower() for w in result["policy_warnings"])


def test_policy_guard_extracts_evidence_urls():
    """Test policy guard extracts and deduplicates evidence URLs."""
    config = OrchestratorConfig(policy_mode=PolicyMode.NEUTRAL_STRICT, max_sources=5)
    state = {
        "question": "Alle Mandate",
        "intent": UserIntent(
            intent_type=IntentType.MANDATES_LIST,
            strict_evidence=True,
            parliament_id="nds",
        ),
        "tool_results": [
            {
                "tool_name": "mandates.search",
                "data": {
                    "rows": [
                        {
                            "mandate_id": "mandate-1",
                            "evidence_urls": [
                                "https://example.com/evidence1",
                                "https://example.com/evidence2",
                            ],
                        },
                        {
                            "mandate_id": "mandate-2",
                            "evidence_urls": [
                                "https://example.com/evidence1",
                                "https://example.com/evidence3",
                            ],
                        },
                    ]
                },
            }
        ],
        "computed": ComputedResult().model_dump(),
    }

    result = policy_guard_node(state, config)

    sources = result["safe_answer_plan"]["sources"]
    assert len(sources) == 3
    assert "https://example.com/evidence1" in sources
    assert "https://example.com/evidence2" in sources
    assert "https://example.com/evidence3" in sources
    assert len(sources) <= config.max_sources


def test_policy_guard_off_mode():
    """Test policy guard in OFF mode."""
    config = OrchestratorConfig(policy_mode=PolicyMode.OFF)
    state = {
        "question": "Wer ist die schlimmste Partei?",
        "intent": UserIntent(
            intent_type=IntentType.LEGISLATURE_STATS,
            strict_evidence=True,
        ),
        "tool_results": [],
        "computed": None,
    }

    result = policy_guard_node(state, config)

    assert result["policy_decision"] == PolicyDecision.PASS
    assert len(result["policy_warnings"]) == 0


def test_policy_guard_safe_answer_plan_structure():
    """Test policy guard creates proper safe answer plan."""
    config = OrchestratorConfig(policy_mode=PolicyMode.NEUTRAL_STRICT, response_sections=True)
    state = {
        "question": "Alle SPD-Mitglieder im Landtag Niedersachsen zwischen 2014-01-01 und 2020-12-31",
        "intent": UserIntent(
            intent_type=IntentType.MANDATES_LIST,
            strict_evidence=True,
            parliament_id="nds",
            from_date=date(2014, 1, 1),
            to_date=date(2020, 12, 31),
        ),
        "tool_results": [
            {
                "tool_name": "mandates.search",
                "data": {
                    "rows": [
                        {
                            "mandate_id": "mandate-1",
                            "evidence_urls": ["https://example.com/evidence1"],
                        }
                    ]
                },
            }
        ],
        "computed": ComputedResult(
            computed_metrics={"total_seats": 100}
        ).model_dump(),
    }

    result = policy_guard_node(state, config)

    plan = result["safe_answer_plan"]
    assert "sections" in plan
    assert "sources" in plan
    assert "scope" in plan
    assert "computations" in plan
    assert plan["scope"]["parliament_id"] == "nds"
    assert plan["scope"]["from_date"] == "2014-01-01"
    assert plan["scope"]["to_date"] == "2020-12-31"
    assert "Ergebnis" in plan["sections"]
    assert "Quellen" in plan["sections"]

