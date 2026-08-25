"""Case: the simplest shape — entry_consumer, a strictly sequential chain of
two jobs (bare-string producer route = single unconditional destination),
ending in a terminal job (producer: null). Also demonstrates headers:
`ingest` sets one via HandlerResult, `transform` reads it off the message
it consumes."""

from __future__ import annotations

from typing import Any

from mira_flow.transport import Message
from mira_flow.wiring import HandlerResult, handler


@handler("linear_ingest_handler")
def ingest(message: Message, params: dict[str, Any]) -> HandlerResult:
    text = message.payload.get("text", "")
    return HandlerResult(
        payload={"text": text, "length": len(text)},
        headers={"source": "linear-pipeline-trigger"},
    )


@handler("linear_transform_handler")
def transform(message: Message, params: dict[str, Any]) -> dict[str, Any] | None:
    # task_run_id is always None here: it's only set by orchestrator's
    # ledgered path, and this runs via core.WorkflowManager (no DB).
    print(
        f"[linear-pipeline] final: {message.payload} "
        f"(headers={message.headers}, task_run_id={message.task_run_id})"
    )
    return None
