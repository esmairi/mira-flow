#!/usr/bin/env python3
"""Publishes to ex-retries-tasks, to watch `flaky_handler` fail twice then
succeed on the 3rd attempt (fail_until_attempt=3 in workflows/retries.json's
job params) — see the retry_delay_seconds=2 gap between attempts in the logs.
"""

from __future__ import annotations

import argparse
import random

from mira_flow.transport import KafkaBroker, Message

TOPIC = "ex-retries-tasks"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-servers", default=None)
    args = parser.parse_args()

    job_id = f"demo-{random.randint(0, 999_999):06d}"
    broker = KafkaBroker(bootstrap_servers=args.bootstrap_servers)
    try:
        message = Message(topic=TOPIC, payload={"job": job_id})
        broker.publish(TOPIC, message)
    finally:
        broker.close()
    print(f"published to {TOPIC} (id={message.id}) — watch the worker logs for the retry loop")


if __name__ == "__main__":
    main()
