"""Tool Executor Node: HTTP calls with contract validation."""

from typing import Any

from langgraph_app.config import OrchestratorConfig
from langgraph_app.schemas import ToolCall, ToolResult
from langgraph_app.tools_client import ToolsClient


def tool_executor_node(state: dict[str, Any], config: OrchestratorConfig) -> dict[str, Any]:
    """LangGraph node: Execute tool calls."""
    tool_calls = state.get("tool_calls", [])
    if not tool_calls:
        return {"tool_results": []}

    client = ToolsClient(config)
    tool_results: list[dict[str, Any]] = []

    try:
        for tc_dict in tool_calls:
            tc = ToolCall(**tc_dict)
            tool_name = tc.tool_name
            params = tc.params

            try:
                if tool_name == "mandates.search":
                    result_data = client.mandates_search(**params)
                elif tool_name == "legislature.stats":
                    result_data = client.legislature_stats(**params)
                elif tool_name == "person.lookup":
                    result_data = client.person_lookup(**params)
                else:
                    raise ValueError(f"Unknown tool: {tool_name}")

                request_id = result_data.get("meta", {}).get("request_id")

                tool_results.append(
                    ToolResult(
                        tool_name=tool_name,
                        request_id=request_id,
                        data=result_data,
                    ).model_dump()
                )
            except Exception as e:
                tool_results.append(
                    ToolResult(
                        tool_name=tool_name,
                        error=str(e),
                    ).model_dump()
                )
    finally:
        client.close()

    return {"tool_results": tool_results}






