"""FastAPI dependencies — a DB session per request, and app-lifetime state
(workflow configs, the shared publish-only broker) read off `Request.app.state`,
populated once in `app.py`'s lifespan."""

from __future__ import annotations

from collections.abc import Iterator

from fastapi import Request
from sqlalchemy.orm import Session

from mira_flow.ledger import session_scope
from mira_flow.transport import Broker
from mira_flow.wiring import WorkflowConfig


def get_session() -> Iterator[Session]:
    """Yields a DB session for the duration of one request.

    Yields:
        A SQLAlchemy session (committed on success, rolled back on error —
        see `ledger.session_scope`).
    """
    with session_scope() as session:
        yield session


def get_workflow_configs(request: Request) -> list[WorkflowConfig]:
    """Returns the workflow configs loaded at app startup.

    Args:
        request: The current request, for `app.state` access.

    Returns:
        Every workflow config loaded from `ApiServerSettings.workflows_dir`.
    """
    return request.app.state.workflow_configs


def get_broker(request: Request) -> Broker:
    """Returns the app-lifetime broker used to publish retry messages.

    Args:
        request: The current request, for `app.state` access.

    Returns:
        The shared broker.
    """
    return request.app.state.broker
