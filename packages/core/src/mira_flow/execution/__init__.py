"""Execution — shared contracts/helpers for running a job, used by both
`core` and `orchestrator` (which never import each other). Neither `transport`
("how messages move") nor `wiring` ("which topic feeds which job") honestly
describes "the shape of a job's execution loop" or "which declared route a
handler's output matches" — that's what lives here instead.
"""

from mira_flow.execution.routing import NoMatchingRouteError, ResolvedRoute, select_route
from mira_flow.execution.worker import Worker

__all__ = ["NoMatchingRouteError", "ResolvedRoute", "Worker", "select_route"]
