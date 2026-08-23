"""Case: `instances: 3` on one consumer — three JobWorker threads, each its
own Consumer client, all sharing one consumer group so Kafka splits the
topic's partitions across them (see ConsumerSpec.instances' docstring).

This only fans out across all 3 if the topic actually has >= 3 partitions —
Kafka auto-creates a topic with 1 partition by default, in which case every
message lands on the same instance and the other two just sit idle. Create
ex-parallel-tasks with 3 partitions first (e.g. via kafka-ui) to see all
three instances get work.
"""

from __future__ import annotations

import threading
from typing import Any

from mira_flow.transport import Message
from mira_flow.wiring import handler


@handler("process_task_handler")
def process_task(message: Message, params: dict[str, Any]) -> dict[str, Any] | None:
    print(f"[{threading.current_thread().name}] processing task {message.payload}")
    return None
