#!/usr/bin/env python3
"""Publishes to ex-avro-orders-in, either schema-conforming (default) or
deliberately missing "amount" (--invalid) to trigger the consumer-side
poison-pill path (see handlers/handlers.py's docstring).

Deliberately uses KafkaBroker with no producer_schemas attached — this
script plays the role of an external, schema-ignorant producer, so an
--invalid payload actually reaches the broker instead of being rejected
client-side before publish.
"""

from __future__ import annotations

import argparse
import json
import random
import uuid

from mira_flow.transport import KafkaBroker, Message

TOPIC = "ex-avro-orders-in"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--invalid",
        action="store_true",
        help="omit \"amount\" to trigger the consumer-side poison-pill skip",
    )
    parser.add_argument("--order-id", default=None, help="random if omitted")
    parser.add_argument("--amount", type=float, default=None, help="random if omitted")
    parser.add_argument("--bootstrap-servers", default=None)
    args = parser.parse_args()

    payload = {"order_id": args.order_id or f"order-{uuid.uuid4().hex[:8]}"}
    if not args.invalid:
        amount = args.amount if args.amount is not None else round(random.uniform(1, 1000), 2)
        payload["amount"] = amount

    broker = KafkaBroker(bootstrap_servers=args.bootstrap_servers)
    try:
        message = Message(topic=TOPIC, payload=payload)
        broker.publish(TOPIC, message)
    finally:
        broker.close()
    kind = "invalid (missing amount)" if args.invalid else "valid"
    print(f"published {kind} {json.dumps(payload)} to {TOPIC} (id={message.id})")


if __name__ == "__main__":
    main()
