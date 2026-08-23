"""GET /runs/{run_id}/tasks — task_run attempts for one run, via
WorkflowRepository's read methods, plus POST .../retry to re-run a FAILED
one.

Retry never touches the DB directly: it republishes the task's original
input to its job's input topic with `Message.force_retry=True`, and lets
the orchestrator's already-running JobWorker do the actual ledger write
(via `WorkflowRepository.start_task_run(force=True)`) exactly the way it
would for a first attempt. That keeps this API read-mostly, avoids
duplicating the execution loop's ledger-write invariants here, and can't
race a worker that's concurrently processing the same task_run.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from mira_flow.api_server.dependencies import get_broker, get_session, get_workflow_configs
from mira_flow.api_server.schemas import RetryResponse, TaskRunDetail, TaskRunSummary
from mira_flow.ledger import TaskRun, WorkflowRepository
from mira_flow.transport import Broker, Message
from mira_flow.wiring import WorkflowConfig

router = APIRouter(prefix="/runs/{run_id}/tasks", tags=["tasks"])

_RETRYABLE_STATUSES = ("FAILED",)


@router.get("", response_model=list[TaskRunSummary])
def list_tasks(run_id: str, session: Session = Depends(get_session)) -> list[TaskRun]:
    """Lists every attempt of every task within one run.

    Args:
        run_id: The run to list attempts for.
        session: Injected DB session.

    Returns:
        Matching attempts, ordered by task name then attempt number.

    Raises:
        HTTPException: 404 if no run has that ID.
    """
    repo = WorkflowRepository(session)
    if repo.get_workflow_run(run_id) is None:
        raise HTTPException(404, f"workflow_run {run_id!r} not found")
    return repo.list_task_runs(run_id)


@router.get("/{task_run_id}", response_model=TaskRunDetail)
def get_task(run_id: str, task_run_id: str, session: Session = Depends(get_session)) -> TaskRun:
    """Gets one task_run attempt, with its full input/output/error.

    Args:
        run_id: The parent run's ID.
        task_run_id: The attempt's ID.
        session: Injected DB session.

    Returns:
        The matching attempt.

    Raises:
        HTTPException: 404 if no such attempt exists within that run.
    """
    return _get_task_run_or_404(WorkflowRepository(session), run_id, task_run_id)


@router.post("/{task_run_id}/retry", response_model=RetryResponse)
def retry_task(
    run_id: str,
    task_run_id: str,
    session: Session = Depends(get_session),
    configs: list[WorkflowConfig] = Depends(get_workflow_configs),
    broker: Broker = Depends(get_broker),
) -> RetryResponse:
    """Retries one FAILED task by republishing its input, force_retry=True.

    Args:
        run_id: The parent run's ID.
        task_run_id: The attempt to retry.
        session: Injected DB session.
        configs: Injected workflow configs, to resolve the job's input topic.
        broker: Injected broker to publish the retry message on.

    Returns:
        Where the retry message was published.

    Raises:
        HTTPException: 404 if the run/task doesn't exist; 409 if the task
            isn't FAILED, its run is CANCELLED, or its workflow/job isn't
            in the loaded configs (e.g. the config changed since the run
            started).
    """
    repo = WorkflowRepository(session)
    task_run = _get_task_run_or_404(repo, run_id, task_run_id)
    if task_run.status not in _RETRYABLE_STATUSES:
        raise HTTPException(
            409, f"task_run {task_run_id!r} is {task_run.status}, not FAILED — nothing to retry"
        )
    run = repo.get_workflow_run(run_id)
    assert run is not None  # guaranteed by _get_task_run_or_404 above
    if run.status == "CANCELLED":
        raise HTTPException(409, f"workflow_run {run_id!r} is CANCELLED — nothing to retry")
    topic = _resolve_input_topic(
        configs, run.workflow_name, run.workflow_version, task_run.task_name
    )

    message = Message(
        topic=topic,
        payload=task_run.input or {},
        workflow_run_id=run.id,
        correlation_id=task_run.correlation_id,
        force_retry=True,
    )
    broker.publish(topic, message)
    return RetryResponse(
        task_run_id=task_run.id, workflow_run_id=run.id, topic=topic, message_id=message.id
    )


def _get_task_run_or_404(repo: WorkflowRepository, run_id: str, task_run_id: str) -> TaskRun:
    """Fetches one task_run, scoped to its parent run.

    Args:
        repo: The repository to query.
        run_id: The parent run's ID.
        task_run_id: The attempt's ID.

    Returns:
        The matching attempt.

    Raises:
        HTTPException: 404 if no such attempt exists within that run.
    """
    task_run = repo.get_task_run(task_run_id)
    if task_run is None or task_run.workflow_run_id != run_id:
        raise HTTPException(404, f"task_run {task_run_id!r} not found in run {run_id!r}")
    return task_run


def _resolve_input_topic(
    configs: list[WorkflowConfig], workflow_name: str, workflow_version: int, task_name: str
) -> str:
    """Finds the Kafka topic a job consumes from, in the currently loaded configs.

    Args:
        configs: Every loaded workflow config.
        workflow_name: The run's workflow.
        workflow_version: The run's config version.
        task_name: The job to resolve.

    Returns:
        That job's input topic.

    Raises:
        HTTPException: 409 if no loaded config matches, or it no longer has
            a job by that name — the config changed since the run started.
    """
    config = next(
        (c for c in configs if c.name == workflow_name and c.version == workflow_version), None
    )
    if config is None:
        raise HTTPException(
            409,
            f"no loaded config matches workflow {workflow_name!r} v{workflow_version}; "
            "can't resolve its input topic to retry",
        )
    job = next((j for j in config.jobs if j.name == task_name), None)
    if job is None:
        raise HTTPException(
            409, f"workflow {workflow_name!r} v{workflow_version} has no job named {task_name!r}"
        )
    consumer = next(c for c in config.consumers if c.name == job.consumer)
    return consumer.topic
