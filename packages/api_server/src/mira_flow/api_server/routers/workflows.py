"""GET /workflows — the configured workflows, from the JSON configs loaded
at startup (not the ledger, so a workflow with zero runs still shows up).
POST /workflows/{name}/trigger — start a new run, the same way `event-
conductor trigger` does from the CLI."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from mira_flow.api_server.dependencies import get_broker, get_session, get_workflow_configs
from mira_flow.api_server.schemas import (
    TriggerRequest,
    TriggerResponse,
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

    run = WorkflowRepository(session).get_or_create_workflow_run(
        workflow_name=config.name,
        workflow_version=config.version,
        idempotency_key=body.idempotency_key,
        input=body.input,
    )
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
