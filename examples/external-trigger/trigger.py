#!/usr/bin/env python3
"""Publishes to ex-external-events — the *only* way to start this workflow,
since it declares no entry_consumer (`mira-flow trigger`/`WorkflowManager.
trigger()` both refuse: there's no entry topic to publish to). Stands in
for the external service this workflow expects to be fed by, including a
custom Kafka header (see handlers/handlers.py, which prints it back).
"""

from __future__ import annotations

import argparse
import json
import random

from mira_flow.transport import KafkaBroker, Message

TOPIC = "ex-external-events"

_EVENTS = ["user_signup", "user_login", "user_upgrade", "user_churn"]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--payload", help="override the default (random) JSON object")
    parser.add_argument("--bootstrap-servers", default=None)
    args = parser.parse_args()

    default_payload = {
        "event": random.choice(_EVENTS),
        "user_id": f"user-{random.randint(1000, 9999)}",
    }
    payload = json.loads(args.payload) if args.payload else default_payload

    broker = KafkaBroker(bootstrap_servers=args.bootstrap_servers)
    try:
        message = Message(topic=TOPIC, payload=payload, headers={"source": "external-service"})
        broker.publish(TOPIC, message)
    finally:
        broker.close()
    print(f"published {json.dumps(payload)} to {TOPIC} (id={message.id})")


if __name__ == "__main__":
    main()
