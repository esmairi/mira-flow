"""API response models — read-only views over WorkflowConfig / WorkflowRun / TaskRun."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from mira_flow.wiring import ConsumerSpec, WorkflowConfig


class RouteSummary(BaseModel):
    """One of a job's output routes, in priority order.

    Attributes:
        name: The producer name (`producers[].name`).
        topic: That producer's Kafka topic.
        when: The route's condition as declared in the config
            (`{"field","op","value"}` / `{"all": [...]}` / `{"any": [...]}`);
            None means it always matches.
    """

    model_config = ConfigDict(frozen=True)

    name: str
    topic: str
    when: dict[str, Any] | None


class JobSummary(BaseModel):
    """One job within a workflow, as declared in its config.

    Attributes:
        name: The job's name.
        handler: The registered handler name it runs.
        consumer: The consumer name it reads from (`ConsumerSpec.name`).
        input_topic: That consumer's Kafka topic.
        producer_routes: The producer names its output may route to; empty
            means terminal.
        output_topics: The Kafka topics of those producers, in the same
            order — lets a UI link jobs into a graph by topic.
        routes: The same routes with their topic and `when` condition, in
            the order they're tried (first match wins).
        retries: Max retry attempts on handler failure.
        retry_delay_seconds: Delay between retry attempts.
        is_entry: Whether this job is the workflow's `entry_consumer`.
    """

    model_config = ConfigDict(frozen=True)

    name: str
    handler: str
    consumer: str
    input_topic: str
    producer_routes: tuple[str, ...]
    output_topics: tuple[str, ...] = ()
    routes: tuple[RouteSummary, ...] = ()
    retries: int
    retry_delay_seconds: float
    is_entry: bool


class WorkflowSummary(BaseModel):
    """One workflow, as declared in its config file.

    Attributes:
        name: The workflow's name.
        version: The config schema version.
        entry_consumer: The consumer name `trigger` publishes to, if any.
        jobs: The workflow's jobs.
    """

    model_config = ConfigDict(frozen=True)

    name: str
    version: int
    entry_consumer: str | None
    jobs: tuple[JobSummary, ...]


def workflow_summary(config: WorkflowConfig) -> WorkflowSummary:
    """Builds a WorkflowSummary from a parsed WorkflowConfig.

    Args:
        config: The parsed, validated workflow config.

    Returns:
        The API-facing summary.
    """
    consumer_by_name: dict[str, ConsumerSpec] = {c.name: c for c in config.consumers}
    producer_topic: dict[str, str] = {p.name: p.topic for p in config.producers}
    jobs = tuple(
        JobSummary(
            name=job.name,
            handler=job.handler,
            consumer=job.consumer,
            input_topic=consumer_by_name[job.consumer].topic,
            producer_routes=tuple(route.name for route in job.producer),
            output_topics=tuple(producer_topic[route.name] for route in job.producer),
            routes=tuple(
                RouteSummary(
                    name=route.name,
                    topic=producer_topic[route.name],
                    when=route.when.model_dump(mode="json") if route.when else None,
                )
                for route in job.producer
            ),
            retries=job.retries,
            retry_delay_seconds=job.retry_delay_seconds,
            is_entry=(job.consumer == config.entry_consumer),
        )
        for job in config.jobs
    )
    return WorkflowSummary(
        name=config.name,
        version=config.version,
        entry_consumer=config.entry_consumer,
        jobs=jobs,
    )


class WorkflowRunSummary(BaseModel):
    """One workflow_run, without its (potentially large) input/output/error.

    Attributes:
        id: The run's ID.
        workflow_name: The workflow this run belongs to.
        workflow_version: The workflow's config version at trigger time.
        status: PENDING / RUNNING / SUCCEEDED / FAILED / CANCELLED.
        correlation_id: The run's correlation ID.
        created_at: When the run was created.
        started_at: When the run's first task started, if it has.
        ended_at: When the run reached a terminal status, if it has.
    """

    model_config = ConfigDict(from_attributes=True)

    id: str
    workflow_name: str
    workflow_version: int
    status: str
    correlation_id: str | None
    created_at: datetime
    started_at: datetime | None
    ended_at: datetime | None


class WorkflowRunDetail(WorkflowRunSummary):
    """A WorkflowRunSummary plus its payloads.

    Attributes:
        idempotency_key: The key `trigger` was called with, if any.
        input: The trigger payload.
        output: The terminal task's output, once SUCCEEDED.
        error: The failing task's error, once FAILED.
        lock_version: The optimistic-concurrency counter (§6).
    """

    idempotency_key: str | None
    input: dict[str, Any]
    output: dict[str, Any] | None
    error: dict[str, Any] | None
    lock_version: int


class TaskRunSummary(BaseModel):
    """One task_run attempt, without its (potentially large) input/output/error.

    Attributes:
        id: The task_run's ID.
        workflow_run_id: The run this attempt belongs to.
        task_name: The job this attempt belongs to.
        attempt: The 1-indexed attempt number.
        status: RUNNING / SUCCEEDED / UP_FOR_RETRY / FAILED.
        correlation_id: The parent run's correlation ID.
        scheduled_at: When this attempt was scheduled to run, if retried.
        started_at: When this attempt started.
        ended_at: When this attempt reached a terminal status, if it has.
        next_retry_at: When the next attempt is scheduled, if UP_FOR_RETRY.
    """

    model_config = ConfigDict(from_attributes=True)

    id: str
    workflow_run_id: str
    task_name: str
    attempt: int
    status: str
    correlation_id: str | None
    scheduled_at: datetime | None
    started_at: datetime | None
    ended_at: datetime | None
    next_retry_at: datetime | None


class TaskRunDetail(TaskRunSummary):
    """A TaskRunSummary plus its payloads.

    Attributes:
        input: This attempt's input payload.
        output: This attempt's output, once SUCCEEDED.
        error: This attempt's error, once FAILED or UP_FOR_RETRY.
    """

    input: dict[str, Any] | None
    output: dict[str, Any] | None
    error: dict[str, Any] | None


class GridRun(WorkflowRunSummary):
    """One run in a grid page, with each task's latest attempt.

    Attributes:
        tasks: The latest attempt of every task that has started in this
            run (tasks never reached aren't listed).
    """

    tasks: list[TaskRunSummary]


class GridResponse(BaseModel):
    """One page of a workflow's grid: runs × tasks, in a single response.

    Attributes:
        runs: The page's runs, newest `created_at` first.
        has_more: Whether older runs exist beyond this page.
    """

    runs: list[GridRun]
    has_more: bool


class RetryResponse(BaseModel):
    """The result of a successful retry request.

    Attributes:
        task_run_id: The task_run that was retried.
        workflow_run_id: Its parent run.
        topic: The Kafka topic the retry message was published to.
        message_id: The republished message's ID.
    """

    model_config = ConfigDict(frozen=True)

    task_run_id: str
    workflow_run_id: str
    topic: str
    message_id: str


class TriggerRequest(BaseModel):
    """A request to start a new workflow_run.

    Attributes:
        idempotency_key: Repeat triggers with the same key return the
            original run instead of creating a new one.
        input: The trigger payload, published to the entry job's topic.
    """

    model_config = ConfigDict(frozen=True)

    idempotency_key: str
    input: dict[str, Any] = Field(default_factory=dict)


class TriggerResponse(BaseModel):
    """The result of a trigger request.

    Attributes:
        workflow_run_id: The new (or, for a repeated idempotency_key,
            pre-existing) run.
        correlation_id: The run's correlation ID.
        topic: The workflow's entry topic.
        message_id: The published message's ID; None when nothing was
            published (see `published`).
        created: Whether this request created the run (False for a
            repeated idempotency_key).
        published: Whether a trigger message was published. A repeated key
            only re-publishes while the run is still PENDING — no task has
            started, so the first message may never have gone out; once the
            run is RUNNING or finished, re-publishing would only replay it.
    """

    model_config = ConfigDict(frozen=True)

    workflow_run_id: str
    correlation_id: str | None
    topic: str
    message_id: str | None
    created: bool
    published: bool
