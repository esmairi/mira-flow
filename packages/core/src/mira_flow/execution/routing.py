"""ResolvedRoute + select_route — a job's producer routes with names already
resolved to topics, and the runtime logic that picks one given a handler's
output. Shared by core.job_worker and orchestrator.job_worker — the two
don't share a full loop, just this piece + Worker.

Not in `wiring`: `wiring.ProducerRoute` (name-based, declared in JSON) is a
config-parsing concept; `ResolvedRoute` (topic-based, built at binding/worker
construction time) is a runtime one — `wiring/config.py` itself never
constructs one, only `core.manager` and `core.bindings` do.
"""

from __future__ import annotations

import logging
from typing import Any

from pydantic import BaseModel, ConfigDict

from mira_flow.wiring import Condition, evaluate_condition

logger = logging.getLogger(__name__)


class NoMatchingRouteError(Exception):
    """Raised at runtime when a job's output matched none of its declared
    producer routes (§2/§5) — a config/handler-output mismatch, not a
    transient failure; not retried, same as a record-or-publish failure."""


class ResolvedRoute(BaseModel):
    """A job's ProducerRoute with its producer `name` resolved to an actual
    Kafka topic — everything downstream of config-loading (both
    `orchestrator` and `core`) operates on topics, never names."""

    model_config = ConfigDict(frozen=True)

    topic: str
    when: Condition | None


def select_route(routes: tuple[ResolvedRoute, ...], payload: dict[str, Any]) -> str | None:
    """Picks the first route whose `when` matches `payload`.

    Routes are evaluated in declaration order; a route with no `when`
    always matches.

    Args:
        routes: The job's resolved producer routes.
        payload: The handler's output to match against.

    Returns:
        The matching route's topic, or None if `routes` is empty (terminal
        job) or, for a non-empty tuple, if nothing matched.
    """
    for route in routes:
        if route.when is None or evaluate_condition(route.when, payload):
            logger.debug("route matched: %s (when=%s)", route.topic, route.when)
            return route.topic
        logger.debug("route rejected: %s (when=%s)", route.topic, route.when)
    return None
