#!/usr/bin/env python3
"""Publishes several messages to ex-parallel-tasks, to watch the 3
JobWorker instances (see workflows/parallel-instances.json's `instances: 3`)
split the work — only actually fans out if the topic has >= 3 partitions
(see handlers/handlers.py's docstring).
"""

from __future__ import annotations

import argparse
import random

from mira_flow.transport import KafkaBroker, Message

TOPIC = "ex-parallel-tasks"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--count", type=int, default=10, help="number of tasks to publish")
    parser.add_argument("--bootstrap-servers", default=None)
    args = parser.parse_args()

    broker = KafkaBroker(bootstrap_servers=args.bootstrap_servers)
    try:
        for _ in range(args.count):
            task_id = f"task-{random.randint(0, 999_999):06d}"
            message = Message(topic=TOPIC, payload={"task_id": task_id})
            broker.publish(TOPIC, message)
            print(f"published task_id={task_id} to {TOPIC} (id={message.id})")
    finally:
        broker.close()


if __name__ == "__main__":
    main()
