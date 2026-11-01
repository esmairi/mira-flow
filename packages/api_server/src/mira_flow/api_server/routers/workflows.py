"""GET /workflows — the configured workflows, from the JSON configs loaded
at startup (not the ledger, so a workflow with zero runs still shows up).
POST /workflows/{name}/trigger — start a new run, the same way `event-
conductor trigger` does from the CLI."""

from __future__ import annotations

from collections import defaultdict

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from mira_flow.api_server.dependencies import get_broker, get_session, get_workflow_configs
from mira_flow.api_server.schemas import (
    GridResponse,
    GridRun,
    TaskRunSummary,
    TriggerRequest,
    TriggerResponse,
    WorkflowRunSummary,
    WorkflowSummary,
    workflow_summary,
)
from mira_flow.ledger import WorkflowRepository
from mira_flow.transport import Broker, Message
from mira_flow.wiring import WorkflowConfig

router = APIRouter(prefix="/workflows", tags=["workflows"])


@router.get("", response_model=list[WorkflowSummary])
def list_workflows(
    configs: list[WorkflowConfig] = Depends(get_workflow_configs),
) -> list[WorkflowSummary]:
    """Lists every configured workflow.

    Args:
        configs: Injected workflow configs.

    Returns:
        One summary per configured workflow.
    """
    return [workflow_summary(config) for config in configs]


@router.get("/{name}", response_model=WorkflowSummary)
def get_workflow(
    name: str, configs: list[WorkflowConfig] = Depends(get_workflow_configs)
) -> WorkflowSummary:
    """Gets one configured workflow by name.

    Args:
        name: The workflow's name.
        configs: Injected workflow configs.

    Returns:
        The matching workflow's summary.

    Raises:
        HTTPException: 404 if no configured workflow has that name.
    """
    config = _get_config_or_404(configs, name)
    return workflow_summary(config)


@router.get("/{name}/grid", response_model=GridResponse)
def workflow_grid(
    name: str,
    limit: int = Query(10, ge=1, le=100),
    offset: int = Query(0, ge=0),
    configs: list[WorkflowConfig] = Depends(get_workflow_configs),
    session: Session = Depends(get_session),
) -> GridResponse:
    """One page of runs with each task's latest attempt — the whole grid view
    in one request (two queries), instead of one tasks request per run.

    Args:
        name: The workflow's name.
        limit: Runs per page (1-100).
        offset: Runs to skip, newest first.
        configs: Injected workflow configs.
        session: Injected DB session.

    Returns:
        The page's runs (newest first) with their tasks, and whether older
        runs exist.

    Raises:
        HTTPException: 404 if no configured workflow has that name.
    """
    _get_config_or_404(configs, name)
    repo = WorkflowRepository(session)
    # One extra row answers `has_more` without a COUNT query.
    runs = repo.list_workflow_runs(workflow_name=name, limit=limit + 1, offset=offset)
    page = runs[:limit]
    tasks_by_run: dict[str, list[TaskRunSummary]] = defaultdict(list)
    for task_run in repo.list_latest_task_runs([run.id for run in page]):
        tasks_by_run[task_run.workflow_run_id].append(TaskRunSummary.model_validate(task_run))
    return GridResponse(
        runs=[
            GridRun(
                **WorkflowRunSummary.model_validate(run).model_dump(),
                tasks=tasks_by_run[run.id],
            )
            for run in page
        ],
        has_more=len(runs) > limit,
    )


@router.post("/{name}/trigger", response_model=TriggerResponse)
def trigger_workflow(
    name: str,
    body: TriggerRequest,
    configs: list[WorkflowConfig] = Depends(get_workflow_configs),
    session: Session = Depends(get_session),
    broker: Broker = Depends(get_broker),
) -> TriggerResponse:
    """Starts a new workflow_run by publishing to the entry job's topic.

    Args:
        name: The workflow to trigger.
        body: The idempotency key and input payload.
        configs: Injected workflow configs.
        session: Injected DB session.
        broker: Injected broker to publish the trigger message on.

    Returns:
        Where the trigger message was published.

    Raises:
        HTTPException: 404 if no configured workflow has that name; 409 if
            it has no entry_consumer — it must be started by an external
            service publishing directly to its first job's topic instead.
    """
    config = _get_config_or_404(configs, name)
    if config.entry_consumer is None:
        raise HTTPException(
            409,
            f"workflow {name!r} has no entry_consumer; it must be started by "
            "an external service publishing directly to its first job's topic",
        )
    entry_topic = next(c.topic for c in config.consumers if c.name == config.entry_consumer)

    run, created = WorkflowRepository(session).get_or_create_workflow_run(
        workflow_name=config.name,
        workflow_version=config.version,
        idempotency_key=body.idempotency_key,
        input=body.input,
    )
    if not created and run.status != "PENDING":
        # Repeated key for a run that's already under way or finished: its
        # trigger message evidently went out — publishing again would only
        # replay it. (A still-PENDING run gets re-published below: its first
        # publish may have failed, and this retry is how it recovers.)
        return TriggerResponse(
            workflow_run_id=run.id,
            correlation_id=run.correlation_id,
            topic=entry_topic,
            message_id=None,
            created=False,
            published=False,
        )
    # Commit before publishing: `get_session` only commits after this
    # endpoint returns, and a worker consuming the message before then
    # would find no workflow_run row for its workflow_run_id.
    session.commit()
    message = Message(
        topic=entry_topic,
        payload=body.input,
        workflow_run_id=run.id,
        correlation_id=run.correlation_id,
    )
    broker.publish(entry_topic, message)
    return TriggerResponse(
        workflow_run_id=run.id,
        correlation_id=run.correlation_id,
        topic=entry_topic,
        message_id=message.id,
        created=created,
        published=True,
    )


def _get_config_or_404(configs: list[WorkflowConfig], name: str) -> WorkflowConfig:
    """Finds a workflow config by name.

    Args:
        configs: Every loaded workflow config.
        name: The workflow's name.

    Returns:
        The matching config.

    Raises:
        HTTPException: 404 if no configured workflow has that name.
    """
    config = next((c for c in configs if c.name == name), None)
    if config is None:
        raise HTTPException(404, f"workflow {name!r} not found")
    return config
