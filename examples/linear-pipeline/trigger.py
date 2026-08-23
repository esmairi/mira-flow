#!/usr/bin/env python3
"""Publishes one message to ex-linear-raw, kicking off the ingest -> transform chain.

Standalone producer (KafkaBroker directly, not the CLI) so it works against
any running instance of this workflow — `mira-flow run --workflow`,
`mira-flow run --workflows-dir`, or the docker compose `worker` service —
without needing the DB ledger `mira-flow trigger` requires.
"""

from __future__ import annotations

import argparse
import json
import random

from mira_flow.transport import KafkaBroker, Message

TOPIC = "ex-linear-raw"

_WORDS = ["hello", "kafka", "workflow", "mira", "event", "pipeline", "world", "job"]


def _random_text() -> str:
    return " ".join(random.choices(_WORDS, k=random.randint(2, 5)))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("text", nargs="?", default=None, help="text to send; random if omitted")
    parser.add_argument("--bootstrap-servers", default=None)
    args = parser.parse_args()

    text = args.text if args.text is not None else _random_text()
    broker = KafkaBroker(bootstrap_servers=args.bootstrap_servers)
    try:
        message = Message(topic=TOPIC, payload={"text": text})
        broker.publish(TOPIC, message)
    finally:
        broker.close()
    print(f"published {json.dumps(message.payload)} to {TOPIC} (id={message.id})")


if __name__ == "__main__":
    main()
