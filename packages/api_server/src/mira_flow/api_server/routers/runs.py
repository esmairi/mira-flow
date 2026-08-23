"""GET /runs — workflow_run history, via WorkflowRepository's read methods
(§ read queries) — the same single place every other ledger access in this
project goes through, not raw queries scattered across route handlers."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from mira_flow.api_server.dependencies import get_session
from mira_flow.api_server.schemas import WorkflowRunDetail, WorkflowRunSummary
from mira_flow.ledger import InvalidTransitionError, WorkflowRepository, WorkflowRun

router = APIRouter(prefix="/runs", tags=["runs"])


@router.get("", response_model=list[WorkflowRunSummary])
def list_runs(
    workflow_name: str | None = None,
    status: str | None = None,
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    session: Session = Depends(get_session),
) -> list[WorkflowRun]:
    """Lists workflow_runs, newest first.

    Args:
        workflow_name: Only runs for this workflow, if set.
        status: Only runs in this status, if set.
        limit: Max rows to return (1-200).
        offset: Rows to skip, for pagination.
        session: Injected DB session.

    Returns:
        Matching runs, newest `created_at` first.
    """
    repo = WorkflowRepository(session)
    return repo.list_workflow_runs(
        workflow_name=workflow_name, status=status, limit=limit, offset=offset
    )


@router.get("/{run_id}", response_model=WorkflowRunDetail)
def get_run(run_id: str, session: Session = Depends(get_session)) -> WorkflowRun:
    """Gets one workflow_run, with its full input/output/error.

    Args:
        run_id: The run's ID.
        session: Injected DB session.

    Returns:
        The matching run.

    Raises:
        HTTPException: 404 if no run has that ID.
    """
    run = WorkflowRepository(session).get_workflow_run(run_id)
    if run is None:
        raise HTTPException(404, f"workflow_run {run_id!r} not found")
    return run


@router.post("/{run_id}/cancel", response_model=WorkflowRunDetail)
def cancel_run(run_id: str, session: Session = Depends(get_session)) -> WorkflowRun:
    """Cancels a PENDING or RUNNING workflow_run.

    Only marks the ledger — it can't interrupt a JobWorker already
    mid-attempt (no live signal channel to a running process), but it does
    block any future attempt for this run from starting.

    Args:
        run_id: The run to cancel.
        session: Injected DB session.

    Returns:
        The now-CANCELLED run.

    Raises:
        HTTPException: 404 if no run has that ID; 409 if it's already
            terminal (SUCCEEDED/FAILED/CANCELLED).
    """
    try:
        run = WorkflowRepository(session).cancel_workflow_run(run_id)
    except InvalidTransitionError as exc:
        raise HTTPException(409, str(exc)) from exc
    if run is None:
        raise HTTPException(404, f"workflow_run {run_id!r} not found")
    return run
