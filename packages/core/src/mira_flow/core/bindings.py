"""JobBinding: config x registry -> runnable unit."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict

from mira_flow.execution import ResolvedRoute
from mira_flow.wiring import (
    ConsumerSpec,
    HandlerRegistry,
    JobSpec,
    ProducerSpec,
    WorkflowConfig,
    load_avro_schema,
)
from mira_flow.wiring.registry import Handler


class JobBinding(BaseModel):
    """A job's config, fully resolved against its handler and its named
    consumer/producer specs — the runnable unit `core` and `orchestrator`
    build workers from.

    Attributes:
        workflow_name: The workflow this job belongs to.
        workflow_version: The workflow's config version.
        task_name: This job's name within the workflow.
        handler: The resolved Python callable for this job.
        input_topic: The Kafka topic this job consumes from.
        output_routes: This job's possible next topics, in priority order —
            empty means terminal. The first route whose `when` matches the
            handler's output (or has no `when`) wins (§2/§5); a non-empty
            tuple with no match is a runtime error (NoMatchingRouteError),
            not a silently-dropped message.
        params: Static params passed to the handler on every call.
        retries: Max retry attempts on handler failure.
        retry_delay_seconds: Delay between retry attempts.
        is_entry: Whether this job is the workflow's entry_consumer.
        consumer_group: This job's input topic's ConsumerSpec.group.
        consumer_params: This job's input topic's ConsumerSpec.params —
            extra confluent_kafka Consumer config (§2).
        producer_params: Per-topic ProducerSpec.params for every topic in
            output_routes, keyed by topic. Empty if terminal.
        consumer_instances: This job's input topic's ConsumerSpec.instances
            — how many parallel worker threads (each its own Consumer
            client, same group) to run (§2).
        input_schema: input_topic's parsed Avro schema
            (ConsumerSpec.schema_avro_value), if configured — used to
            validate every consumed payload. None skips validation.
        output_schemas: Per-topic parsed Avro schema for every topic in
            output_routes, keyed by topic — used to validate every
            published payload. A topic missing here isn't validated.
    """

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    workflow_name: str
    workflow_version: int
    task_name: str
    handler: Handler
    input_topic: str
    output_routes: tuple[ResolvedRoute, ...]
    params: dict[str, Any]
    retries: int
    retry_delay_seconds: float
    is_entry: bool
    consumer_group: str
    consumer_params: dict[str, Any]
    producer_params: dict[str, dict[str, Any]]
    consumer_instances: int
    input_schema: dict[str, Any] | None
    output_schemas: dict[str, dict[str, Any]]


def build_bindings(
    configs: list[WorkflowConfig], registry: HandlerRegistry, base_dir: Path
) -> list[JobBinding]:
    """Resolves every job in every config into a runnable JobBinding.

    Args:
        configs: The workflow configs to build bindings for.
        registry: Resolves each job's `handler` name to a callable.
        base_dir: Directory a consumer/producer's schema_avro_value is
            resolved relative to (the workflows directory).

    Returns:
        One JobBinding per job, across all configs.
    """
    bindings: list[JobBinding] = []
    for config in configs:
        consumer_by_name = {c.name: c for c in config.consumers}
        producer_by_name = {p.name: p for p in config.producers}
        for job in config.jobs:
            bindings.append(
                _build_binding(config, job, consumer_by_name, producer_by_name, registry, base_dir)
            )
    return bindings


def _build_binding(
    config: WorkflowConfig,
    job: JobSpec,
    consumer_by_name: dict[str, ConsumerSpec],
    producer_by_name: dict[str, ProducerSpec],
    registry: HandlerRegistry,
    base_dir: Path,
) -> JobBinding:
    """Resolves one job's config into a JobBinding.

    Args:
        config: The workflow this job belongs to.
        job: The job to resolve.
        consumer_by_name: All of the workflow's consumers, keyed by name.
        producer_by_name: All of the workflow's producers, keyed by name.
        registry: Resolves `job.handler` to a callable.
        base_dir: See `build_bindings`.

    Returns:
        The resolved binding.
    """
    consumer_spec = consumer_by_name[job.consumer]
    output_routes = tuple(
        ResolvedRoute(topic=producer_by_name[route.name].topic, when=route.when)
        for route in job.producer
    )
    producer_params = {
        producer_by_name[route.name].topic: producer_by_name[route.name].params
        for route in job.producer
    }
    output_schemas = {
        producer_by_name[route.name].topic: schema
        for route in job.producer
        if (schema := load_avro_schema(producer_by_name[route.name].schema_avro_value, base_dir))
        is not None
    }
    return JobBinding(
        workflow_name=config.name,
        workflow_version=config.version,
        task_name=job.name,
        handler=registry.resolve(job.handler),
        input_topic=consumer_spec.topic,
        output_routes=output_routes,
        params=job.params,
        retries=job.retries,
        retry_delay_seconds=job.retry_delay_seconds,
        is_entry=(job.consumer == config.entry_consumer),
        consumer_group=consumer_spec.group,
        consumer_params=consumer_spec.params,
        producer_params=producer_params,
        consumer_instances=consumer_spec.instances,
        input_schema=load_avro_schema(consumer_spec.schema_avro_value, base_dir),
        output_schemas=output_schemas,
    )
