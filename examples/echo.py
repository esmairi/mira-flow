"""Demo handlers for the §11 smoke test."""

from __future__ import annotations

from typing import Any

from mira_flow.transport import Message
from mira_flow.wiring import handler


@handler("extract_text_handler")
def extract_text(message: Message, params: dict[str, Any]) -> dict[str, Any] | None:
    n = message.payload.get("n")
    return {"text": f"extracted text for n={n}", "params": params}


@handler("index_document_handler")
def index_document(message: Message, params: dict[str, Any]) -> dict[str, Any] | None:
    print(f"indexed: {message.payload.get('text')}")
    return None
