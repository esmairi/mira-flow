"""Case: no `entry_consumer` at all. This workflow can't be started with
`mira-flow trigger`/`WorkflowManager.trigger()` (both raise: there's no
entry topic to publish to) — it's meant to be fed by an external service
publishing directly to ex-external-events. With `docker compose up -d`
running, produce a message to that topic via kafka-ui
(http://localhost:8080) to see it picked up.
"""

from __future__ import annotations

from typing import Any

from mira_flow.transport import Message
from mira_flow.wiring import handler


@handler("handle_event_handler")
def handle_event(message: Message, params: dict[str, Any]) -> dict[str, Any] | None:
    print(f"[external-trigger] received: {message.payload}")
    return None
