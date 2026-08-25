"""Case: no `entry_consumer` at all. This workflow can't be started with
`mira-flow trigger`/`WorkflowManager.trigger()` (both raise: there's no
entry topic to publish to) — it's meant to be fed by an external service
publishing directly to ex-external-events. With `docker compose up -d`
running, produce a message to that topic via kafka-ui
(http://localhost:8080) to see it picked up.

Also prints any Kafka headers the external publisher set — `message.headers`
holds whatever it sent, minus the reserved `mira-`-prefixed envelope ones
(see transport.Message).
"""

from __future__ import annotations

from typing import Any

from mira_flow.transport import Message
from mira_flow.wiring import handler


@handler("handle_event_handler")
def handle_event(message: Message, params: dict[str, Any]) -> dict[str, Any] | None:
    # task_run_id is always None here: it's only set by orchestrator's
    # ledgered path, and this runs via core.WorkflowManager (no DB).
    print(
        f"[external-trigger] received: {message.payload} "
        f"(headers={message.headers}, task_run_id={message.task_run_id})"
    )
    return None
