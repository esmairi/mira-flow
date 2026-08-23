"""Case: the simplest shape — entry_consumer, a strictly sequential chain of
two jobs (bare-string producer route = single unconditional destination),
ending in a terminal job (producer: null)."""

from __future__ import annotations

from typing import Any

from mira_flow.transport import Message
from mira_flow.wiring import handler


@handler("linear_ingest_handler")
def ingest(message: Message, params: dict[str, Any]) -> dict[str, Any] | None:
    text = message.payload.get("text", "")
    return {"text": text, "length": len(text)}


@handler("linear_transform_handler")
def transform(message: Message, params: dict[str, Any]) -> dict[str, Any] | None:
    print(f"[linear-pipeline] final: {message.payload}")
    return None
