#!/usr/bin/env python3
"""Publishes orders to ex-order-received, one per scenario, each steering the
order-fulfillment workflow down a different path (see handlers/handlers.py's
diagram).

Standalone producer (KafkaBroker directly, not the CLI) so it works against
any running instance of this workflow, without needing the DB ledger
`mira-flow trigger` requires. With the api_server + UI running, the same
payloads can also be pasted into the UI's Trigger dialog.
"""

from __future__ import annotations

import argparse
import json
import random
import uuid
from collections.abc import Callable
from typing import Any

from mira_flow.transport import KafkaBroker, Message

TOPIC = "ex-order-received"


def _order(
    *,
    items: list[dict[str, Any]] | None = None,
    country: str = "FR",
    email: str | None = None,
    shipping: str = "standard",
    gateway: str = "ok",
) -> dict[str, Any]:
    order_id = f"ord-{uuid.uuid4().hex[:8]}"
    return {
        "order_id": order_id,
        "customer": {
            "id": f"cus-{random.randint(1000, 9999)}",
            "email": email or f"{order_id}@example.com",
            "country": country,
        },
        "items": items
        if items is not None
        else [
            {"sku": "BOOK-42", "qty": random.randint(1, 3), "price": 19.9},
            {"sku": "MUG-07", "qty": 1, "price": round(random.uniform(8, 15), 2)},
        ],
        "shipping": shipping,
        "gateway": gateway,
    }


_EXPENSIVE = [{"sku": "LAPTOP-PRO", "qty": 1, "price": 1899.0}]
_HUGE = [{"sku": "SERVER-RACK", "qty": 2, "price": 4500.0}]

# scenario -> (expected path, payload factory)
SCENARIOS: dict[str, tuple[str, Callable[[], dict[str, Any]]]] = {
    "standard": (
        "receive -> fraud(low) -> payment -> ship_standard -> notify_customer",
        lambda: _order(),
    ),
    "express": (
        "receive -> fraud(low) -> payment -> ship_express -> notify_customer",
        lambda: _order(shipping="express"),
    ),
    "review-approved": (
        "receive -> fraud(high) -> manual_review(approve) -> payment -> ship_express -> notify",
        lambda: _order(items=_EXPENSIVE, country="XX", shipping="express"),
    ),
    "review-rejected": (
        "receive -> fraud(high) -> manual_review(reject) -> notify_rejection",
        lambda: _order(items=_HUGE, email="buyer@tempmail.test"),
    ),
    "invalid": (
        "receive(invalid) -> notify_rejection",
        lambda: _order(items=[]),
    ),
    "flaky": (
        "... -> payment (fails once, auto-retried, succeeds) -> ship_standard -> notify",
        lambda: _order(gateway="flaky"),
    ),
    "outage": (
        "... -> payment FAILED after 3 attempts -> hit Retry on it in the UI to finish",
        lambda: _order(gateway="outage"),
    ),
}


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        "scenario",
        nargs="?",
        choices=[*SCENARIOS, "all"],
        default="all",
        help="\n".join(f"{name}: {path}" for name, (path, _) in SCENARIOS.items())
        + "\nall: one of each (default)",
    )
    parser.add_argument("--count", type=int, default=1, help="orders per scenario (default 1)")
    parser.add_argument("--print", action="store_true", help="print payloads only, don't publish")
    parser.add_argument("--bootstrap-servers", default=None)
    args = parser.parse_args()

    names = list(SCENARIOS) if args.scenario == "all" else [args.scenario]
    if args.print:
        for name in names:
            print(f"# {name}: {SCENARIOS[name][0]}\n{json.dumps(SCENARIOS[name][1](), indent=2)}")
        return

    broker = KafkaBroker(bootstrap_servers=args.bootstrap_servers)
    try:
        for name in names:
            path, factory = SCENARIOS[name]
            for _ in range(args.count):
                payload = factory()
                message = Message(topic=TOPIC, payload=payload)
                broker.publish(TOPIC, message)
                print(f"[{name}] {payload['order_id']} published (id={message.id})")
                print(f"    expect: {path}")
    finally:
        broker.close()


if __name__ == "__main__":
    main()
