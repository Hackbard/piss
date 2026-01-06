"""Router Node: Deterministic tool routing."""

from typing import Any

from langgraph_app.schemas import IntentType, ToolCall, UserIntent


def router_node(state: dict[str, Any]) -> dict[str, Any]:
    """LangGraph node: Route intent to tool calls."""
    intent = state.get("intent")
    if not intent:
        return {"tool_calls": []}

    if intent.needs_clarification:
        return {"tool_calls": []}

    tool_calls: list[ToolCall] = []

    if intent.intent_type == IntentType.MANDATES_LIST:
        tool_calls.append(
            ToolCall(
                tool_name="mandates.search",
                params=intent.filters.copy(),
            )
        )

    elif intent.intent_type == IntentType.LEGISLATURE_STATS:
        if intent.legislature_id:
            tool_calls.append(
                ToolCall(
                    tool_name="legislature.stats",
                    params={
                        "legislature_id": intent.legislature_id,
                        "strict_evidence": intent.strict_evidence,
                    },
                )
            )

    elif intent.intent_type == IntentType.COMBINED_MANDATES_AND_STATS:
        tool_calls.append(
            ToolCall(
                tool_name="mandates.search",
                params=intent.filters.copy(),
            )
        )

        if intent.legislature_id:
            tool_calls.append(
                ToolCall(
                    tool_name="legislature.stats",
                    params={
                        "legislature_id": intent.legislature_id,
                        "strict_evidence": intent.strict_evidence,
                    },
                )
            )
        else:
            legislature_ids = set()
            if "legislature_id" in intent.filters:
                legislature_ids.add(intent.filters["legislature_id"])

            for legislature_id in legislature_ids:
                tool_calls.append(
                    ToolCall(
                        tool_name="legislature.stats",
                        params={
                            "legislature_id": legislature_id,
                            "strict_evidence": intent.strict_evidence,
                        },
                    )
                )

    elif intent.intent_type == IntentType.PERSON_LOOKUP:
        if intent.filters.get("person_id"):
            tool_calls.append(
                ToolCall(
                    tool_name="person.lookup",
                    params={
                        "person_id": intent.filters["person_id"],
                    },
                )
            )
        elif intent.person_name_contains:
            tool_calls.append(
                ToolCall(
                    tool_name="person.lookup",
                    params={
                        "name_contains": intent.person_name_contains,
                        "limit": 20,
                    },
                )
            )

    return {"tool_calls": [tc.model_dump() for tc in tool_calls]}

