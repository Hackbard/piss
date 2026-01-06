"""Entrypoint for LangGraph Local Server."""

import asyncio

from langgraph.graph import StateGraph

from langgraph_app.config import get_config
from langgraph_app.graph import create_graph, GraphState


def create_orchestrator_app() -> StateGraph:
    """Create LangGraph orchestrator app."""
    config = get_config()
    return create_graph(config)


async def run_query(question: str) -> str:
    """Run a single query through the orchestrator."""
    app = create_orchestrator_app()
    config = get_config()

    initial_state: GraphState = {
        "question": question,
        "intent": None,
        "tool_calls": [],
        "tool_results": [],
        "computed": None,
        "evidence_gate_passed": True,
        "evidence_gate_error": None,
        "final_answer": None,
    }

    result = await app.ainvoke(initial_state)
    return result.get("final_answer", "No answer generated.")


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python -m langgraph_app.server <question>")
        sys.exit(1)

    question = " ".join(sys.argv[1:])
    answer = asyncio.run(run_query(question))
    print(answer)

