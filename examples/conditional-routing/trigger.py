#!/usr/bin/env python3
"""Publishes to ex-cond-input, to watch `classify_handler`'s routing decide
between the 4 producer routes (flagged/high/medium/low — see workflows/
conditional-routing.json's `when` clauses).

Standalone producer (KafkaBroker directly, not the CLI) so it works against
any running instance of this workflow, without needing the DB ledger
`mira-flow trigger` requires.
"""

from __future__ import annotations

import argparse
import json
import random

from mira_flow.transport import KafkaBroker, Message

TOPIC = "ex-cond-input"

# One payload generator per route, matching examples/README.md's demo — each
# randomizes within the bounds that still hit exactly that producer route
# (see workflows/conditional-routing.json's `when` clauses), so re-running
# needs no flags to get varied-but-correct data.
ROUTE_PAYLOAD_FACTORIES = {
    "flagged": lambda: {  # AnyOf -> ex-cond-flagged
        "score": random.randint(0, 100),
        "category": random.choice(["urgent", "escalate"]),
    },
    "high": lambda: {"score": random.randint(90, 100), "category": "normal"},  # -> ex-cond-high
    "medium": lambda: {"score": random.randint(50, 89), "category": "normal"},  # -> ex-cond-medium
    "low": lambda: {"score": random.randint(0, 49), "category": "normal"},  # catch-all -> low
}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "route",
        nargs="?",
        choices=[*ROUTE_PAYLOAD_FACTORIES, "all"],
        default="all",
        help="which route's payload to send (default: send all 4)",
    )
    parser.add_argument("--payload", help="override with a custom JSON object")
    parser.add_argument("--bootstrap-servers", default=None)
    args = parser.parse_args()

    routes = list(ROUTE_PAYLOAD_FACTORIES) if args.route == "all" else [args.route]
    broker = KafkaBroker(bootstrap_servers=args.bootstrap_servers)
    try:
        for route in routes:
            payload = json.loads(args.payload) if args.payload else ROUTE_PAYLOAD_FACTORIES[route]()
            message = Message(topic=TOPIC, payload=payload)
            broker.publish(TOPIC, message)
            print(f"[{route}] published {json.dumps(payload)} to {TOPIC} (id={message.id})")
    finally:
        broker.close()


if __name__ == "__main__":
    main()
