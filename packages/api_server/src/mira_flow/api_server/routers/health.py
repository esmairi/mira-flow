"""GET /health, /ready — liveness and readiness probes (Kubernetes-style)."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import text
from sqlalchemy.orm import Session

from mira_flow.api_server.dependencies import get_session

router = APIRouter(tags=["health"])


@router.get("/health")
def health() -> dict[str, str]:
    """Liveness probe: is the process up at all.

    Checks nothing beyond the process running — no DB, no broker. Use
    `/ready` for a check that actually exercises a dependency.

    Returns:
        {"status": "ok"}.
    """
    return {"status": "ok"}


@router.get("/ready")
def ready(session: Session = Depends(get_session)) -> dict[str, str]:
    """Readiness probe: can this instance actually serve traffic.

    Only checks the DB — the broker isn't checked here, since a
    request-time round trip to Kafka on every readiness poll would make
    this probe slow and flaky rather than meaningful.

    Args:
        session: Injected DB session.

    Returns:
        {"status": "ready"}.

    Raises:
        HTTPException: 503 if the DB isn't reachable.
    """
    try:
        session.execute(text("SELECT 1"))
    except Exception as exc:  # noqa: BLE001 - report as unready, not a 500
        raise HTTPException(503, f"database not reachable: {exc}") from exc
    return {"status": "ready"}
