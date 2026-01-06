"""Deterministic Compute Node: Python-only calculations."""

from typing import Any

from langgraph_app.schemas import ComputedResult, GroupBy, Metric, UserIntent


def compute_node(state: dict[str, Any]) -> dict[str, Any]:
    """LangGraph node: Perform deterministic computations."""
    intent = state.get("intent")
    tool_results = state.get("tool_results", [])

    if not intent:
        return {"computed": ComputedResult().model_dump()}

    computed_metrics: dict[str, Any] = {}
    grouped_data: dict[str, Any] | None = None
    raw_data: dict[str, Any] = {}

    for tr in tool_results:
        tool_name = tr.get("tool_name", "")
        data = tr.get("data", {})
        raw_data[tool_name] = data

        if tool_name == "legislature.stats":
            total_seats = data.get("total_seats")
            party_seats = data.get("party_seats", {})

            if total_seats and party_seats:
                for metric in intent.metrics:
                    if metric == Metric.SEAT_SHARE_PERCENT:
                        seat_shares: dict[str, float] = {}
                        for party_code, seats in party_seats.items():
                            if total_seats > 0:
                                share = round(seats / total_seats * 100, 1)
                                seat_shares[party_code] = share
                        computed_metrics["seat_share_percent"] = seat_shares

                    elif metric == Metric.TOTAL_SEATS:
                        computed_metrics["total_seats"] = total_seats

                    elif metric == Metric.PARTY_COUNT:
                        computed_metrics["party_count"] = len(party_seats)

        if tool_name == "mandates.search" and intent.group_by:
            rows = data.get("rows", [])

            if intent.group_by == GroupBy.LEGISLATURE:
                grouped: dict[str, list[dict[str, Any]]] = {}
                for row in rows:
                    legislature_id = row.get("legislature_id", "unknown")
                    if legislature_id not in grouped:
                        grouped[legislature_id] = []
                    grouped[legislature_id].append(row)

                grouped_data = {"by_legislature": grouped}

            elif intent.group_by == GroupBy.PARTY:
                grouped: dict[str, list[dict[str, Any]]] = {}
                for row in rows:
                    party_code = row.get("party_code", "unknown")
                    if party_code not in grouped:
                        grouped[party_code] = []
                    grouped[party_code].append(row)

                grouped_data = {"by_party": grouped}

    return {
        "computed": ComputedResult(
            computed_metrics=computed_metrics,
            grouped_data=grouped_data,
            raw_data=raw_data,
        ).model_dump()
    }

