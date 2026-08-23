"""Case: `retries` + `retry_delay_seconds` — a handler that fails a few times
before succeeding, to watch the retry loop in the logs.

Retries are counted in memory per message.id, held in a module-level dict —
fine for this demo (core.JobWorker's retries are in-memory only anyway, see
core/job_worker.py's docstring), but note it means a *redelivered* message
(e.g. after a crash mid-retry) restarts its attempt count from zero rather
than resuming where it left off.
"""

from __future__ import annotations

from typing import Any

from mira_flow.transport import Message
from mira_flow.wiring import handler

_attempts: dict[str, int] = {}


@handler("flaky_handler")
def flaky(message: Message, params: dict[str, Any]) -> dict[str, Any] | None:
    count = _attempts.get(message.id, 0) + 1
    _attempts[message.id] = count
    fail_until_attempt = params.get("fail_until_attempt", 1)
    if count < fail_until_attempt:
        raise RuntimeError(f"simulated transient failure (attempt {count}/{fail_until_attempt})")
    print(f"[retries] succeeded on attempt {count}: {message.payload}")
    return None
