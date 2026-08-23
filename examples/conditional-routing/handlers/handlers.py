"""Case: one job, four producer routes, covering every `when` shape —
a plain Comparison (high-producer), AnyOf (flagged-producer), AllOf
(medium-producer), and a catch-all default with no `when` at all
(low-producer). Routes are tried in declaration order and the first match
wins, so the catch-all must be listed last."""

from __future__ import annotations

from typing import Any

from mira_flow.transport import Message
from mira_flow.wiring import handler


@handler("classify_handler")
def classify(message: Message, params: dict[str, Any]) -> dict[str, Any] | None:
    # Pass-through: routing evaluates the trigger input's own fields directly.
    return message.payload
