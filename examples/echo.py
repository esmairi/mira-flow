"""Demo handlers for the §11 smoke test. Also demonstrates headers:
extract_text sets one via HandlerResult, index_document reads it off the
message it consumes."""

from __future__ import annotations

from typing import Any

from mira_flow.transport import Message
from mira_flow.wiring import HandlerResult, handler


@handler("extract_text_handler")
def extract_text(message: Message, params: dict[str, Any]) -> HandlerResult:
    n = message.payload.get("n")
    return HandlerResult(
        payload={"text": f"extracted text for n={n}", "params": params},
        headers={"source": "extract_text_handler"},
    )


@handler("index_document_handler")
def index_document(message: Message, params: dict[str, Any]) -> dict[str, Any] | None:
    # task_run_id is always None here: it's only set by orchestrator's
    # ledgered path, and this runs via core.WorkflowManager (no DB).
    print(
        f"indexed: {message.payload.get('text')} "
        f"(headers={message.headers}, task_run_id={message.task_run_id})"
    )
    return None
