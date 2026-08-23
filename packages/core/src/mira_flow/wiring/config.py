"""Workflow config files: parsing and all §2 validation."""

from __future__ import annotations

import json
import logging
import operator
from collections.abc import Callable
from pathlib import Path
from typing import Any, Literal

import fastavro
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

logger = logging.getLogger(__name__)

SUPPORTED_CONFIG_VERSIONS = frozenset({1})
"""§8: `version` gates parsing so an old/new-shaped config fails with one
clear ConfigError instead of a confusing generic validation error. Bump when
a breaking schema change ships, and widen this set only once a migration
path for the old version exists."""


class ConfigError(Exception):
    """Raised when a workflow config violates the §2 rules."""


class ConsumerSpec(BaseModel):
    """One named, subscribable Kafka topic (§2 `consumers[]`).

    Attributes:
        name: Referenced by `job.consumer` and `entry_consumer`.
        topic: The Kafka topic this consumer subscribes to.
        group: The consumer group ID; defaults to the workflow name if
            omitted (applied before validation — see `_apply_default_group`).
        schema_avro_value: Optional path (relative to the config file) to an
            Avro schema validating this topic's value shape.
        schema_avro_key: Optional path to an Avro schema for the key shape.
        params: Extra confluent_kafka.Consumer config, passed as kwargs on
            creation. bootstrap.servers / group.id / enable.auto.commit are
            always forced by mira-flow (can't be overridden here);
            auto.offset.reset defaults to "earliest" but params may
            override it.
        instances: How many parallel consumer instances (threads, each its
            own Consumer client) to run for this topic, sharing `group` so
            Kafka splits the topic's partitions across them — safe up to
            the partition count; extra instances beyond that just sit idle
            with none assigned. Safe for our per-run ledger semantics
            regardless of value: Message.key = workflow_run_id, so Kafka
            always routes one run's messages to the same partition, meaning
            at most one instance ever handles a given run.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    name: str
    topic: str
    group: str
    schema_avro_value: str | None = None
    schema_avro_key: str | None = None
    params: dict[str, Any] = Field(default_factory=dict)
    instances: int = Field(default=1, ge=1)


class ProducerSpec(BaseModel):
    """One named, publishable Kafka topic (§2 `producers[]`).

    Attributes:
        name: Referenced by `job.producer[].name`.
        topic: The Kafka topic this producer publishes to.
        schema_avro_value: Optional path (relative to the config file) to an
            Avro schema validating this topic's value shape.
        schema_avro_key: Optional path to an Avro schema for the key shape.
        params: Extra confluent_kafka.Producer config, passed as kwargs on
            creation. bootstrap.servers is always forced by
            mira-flow; acks and enable.idempotence default to
            "all"/True but params may override them.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    name: str
    topic: str
    schema_avro_value: str | None = None
    schema_avro_key: str | None = None
    params: dict[str, Any] = Field(default_factory=dict)


class Comparison(BaseModel):
    """A leaf predicate: one payload field against one literal value.

    Deliberately not a general expression language (no eval, no arbitrary
    code) — combine leaves with AllOf/AnyOf below for boolean structure,
    a job's whole set of possible next steps still stays readable straight
    from the config, which a code-driven routing decision would not.

    Attributes:
        field: The payload key to compare.
        op: The comparison operator.
        value: The literal to compare `field` against.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    field: str
    op: Literal["eq", "ne", "gt", "gte", "lt", "lte", "in", "contains"]
    value: Any


class AllOf(BaseModel):
    """True iff every nested condition is true (boolean AND).

    Attributes:
        all: The conditions to AND together.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    all: tuple[Condition, ...]


class AnyOf(BaseModel):
    """True iff at least one nested condition is true (boolean OR).

    Attributes:
        any: The conditions to OR together.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    any: tuple[Condition, ...]


Condition = Comparison | AllOf | AnyOf
"""A route's `when`: either one field/op/value comparison, or `{"all": [...]}` /
`{"any": [...]}` nesting other Conditions (recursively) for boolean AND/OR.
Pydantic distinguishes the three by shape (disjoint field sets, extra="forbid"),
no discriminator tag needed."""

AllOf.model_rebuild()
AnyOf.model_rebuild()

_COMPARATORS: dict[str, Callable[[Any, Any], bool]] = {
    "eq": operator.eq,
    "ne": operator.ne,
    "gt": operator.gt,
    "gte": operator.ge,
    "lt": operator.lt,
    "lte": operator.le,
    "in": lambda actual, value: actual in value,
    "contains": lambda actual, value: value in actual,
}


def evaluate_condition(condition: Condition, payload: dict[str, Any]) -> bool:
    """Evaluates a Condition (leaf or AllOf/AnyOf composite) against a payload.

    Args:
        condition: The condition to evaluate.
        payload: The handler's returned payload to evaluate it against.

    Returns:
        Whether `condition` matches `payload`. A missing field never
        matches a leaf comparison (rather than raising) — a route with a
        `when` on a field the payload doesn't have is simply not a
        candidate for that output.
    """
    if isinstance(condition, AllOf):
        return all(evaluate_condition(c, payload) for c in condition.all)
    if isinstance(condition, AnyOf):
        return any(evaluate_condition(c, payload) for c in condition.any)
    return _evaluate_comparison(condition, payload)


def _evaluate_comparison(condition: Comparison, payload: dict[str, Any]) -> bool:
    """Evaluates one leaf Comparison against a payload.

    Args:
        condition: The leaf comparison to evaluate.
        payload: The handler's returned payload to evaluate it against.

    Returns:
        False if `condition.field` is absent from `payload`, otherwise the
        result of applying `condition.op`.
    """
    if condition.field not in payload:
        return False
    return _COMPARATORS[condition.op](payload[condition.field], condition.value)


class ProducerRoute(BaseModel):
    """One candidate destination for a job's output.

    Omitted (or the job's `producer` given as a bare string) means "always
    matches", for the common single-destination case.

    Attributes:
        name: References a declared `producers[].name`.
        when: Evaluated against the handler's returned payload; None always
            matches.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    name: str
    when: Condition | None = None


class JobSpec(BaseModel):
    """One job: a handler wired to one input topic and 0+ output routes.

    Attributes:
        name: This job's name within the workflow.
        handler: References a registered `@handler("...")` name.
        consumer: References a declared `consumers[].name`.
        producer: References declared `producers[].name`(s). Normalized
            from JSON's `null` (terminal job, empty tuple) / a bare
            producer name string (single unconditional route) / a list of
            `{name, when}` objects (conditional routing, §2) — see
            `_normalize_producer` below. The first matching route wins; §8
            covers what happens when none match.
        params: Static params passed to the handler on every call.
        retries: Max retry attempts on handler failure.
        retry_delay_seconds: Delay between retry attempts.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    name: str
    handler: str
    consumer: str
    producer: tuple[ProducerRoute, ...] = Field(default_factory=tuple)
    params: dict[str, Any] = Field(default_factory=dict)
    retries: int = Field(default=0, ge=0)
    retry_delay_seconds: float = Field(default=0.0, ge=0)

    @field_validator("producer", mode="before")
    @classmethod
    def _normalize_producer(cls, v: Any) -> Any:
        if v is None:
            return ()
        if isinstance(v, str):
            return ({"name": v, "when": None},)
        return v


class WorkflowConfig(BaseModel):
    """One parsed, structurally-validated workflow config file.

    Attributes:
        name: The workflow's name; also its default consumer group.
        version: Config schema version; must be in SUPPORTED_CONFIG_VERSIONS.
        consumers: Every topic a job in this workflow can consume from.
        producers: Every topic a job in this workflow can publish to.
        jobs: The workflow's jobs; at least one is required.
        entry_consumer: References a declared `consumers[].name` — the
            topic `trigger` publishes to is that consumer's `.topic`.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    name: str
    version: int
    consumers: tuple[ConsumerSpec, ...]
    producers: tuple[ProducerSpec, ...]
    jobs: tuple[JobSpec, ...] = Field(min_length=1)
    entry_consumer: str | None = None


def load_workflows(directory: str | Path) -> list[WorkflowConfig]:
    """Parses and validates every `*.json` workflow config in a directory.

    Args:
        directory: The directory to scan for workflow configs.

    Returns:
        One WorkflowConfig per file, in filename order.

    Raises:
        ConfigError: If `directory` doesn't exist, any file fails to parse
            or validate, or two files' jobs consume the same topic.
    """
    directory = Path(directory)
    if not directory.is_dir():
        raise ConfigError(f"workflows directory not found: {directory}")
    configs = [parse_workflow_file(path) for path in sorted(directory.glob("*.json"))]
    _check_cross_file_consumers(configs)
    return configs


# -- parsing ---------------------------------------------------------------


def parse_workflow_file(path: str | Path) -> WorkflowConfig:
    """Parses and validates a single workflow config file.

    Field-level shape (types, required/optional, numeric bounds, unknown
    keys) is Pydantic's job (the model classes above). What's left here is
    what Pydantic can't do by itself: a JSON-parsing-specific default
    (`group` falling back to the workflow name), a supported-version gate
    (§8), and validation that spans multiple fields/files or touches the
    filesystem (name references, cycle detection, cross-file consumer
    uniqueness, Avro schema files) — see `_validate_workflow` /
    `_validate_avro_schemas` below.

    `load_workflows` uses this per file plus a cross-file consumer-uniqueness
    check (§2); callers that only need one workflow in isolation — e.g. the
    ledger-free `core` package — can call this directly.

    Args:
        path: Path to the workflow's JSON config file.

    Returns:
        The parsed, fully-validated config.

    Raises:
        ConfigError: If the file isn't valid JSON, doesn't match the
            WorkflowConfig shape, has an unsupported version, or fails any
            structural/Avro check.
    """
    path = Path(path)
    try:
        data = json.loads(path.read_text())
    except json.JSONDecodeError as exc:
        raise ConfigError(f"{path}: invalid JSON: {exc}") from exc
    if not isinstance(data, dict):
        raise ConfigError(f"{path}: top level must be a JSON object")

    _apply_default_group(data)

    try:
        config = WorkflowConfig.model_validate(data)
    except ValidationError as exc:
        raise ConfigError(_format_validation_error(path, exc)) from exc

    ctx = str(path)
    if config.version not in SUPPORTED_CONFIG_VERSIONS:
        raise ConfigError(
            f"{ctx}: unsupported config version {config.version} "
            f"(supported: {sorted(SUPPORTED_CONFIG_VERSIONS)})"
        )
    _validate_avro_schemas(config, base_dir=path.parent, ctx=ctx)
    _validate_workflow(config, ctx)
    logger.debug(
        "%s: parsed workflow %r v%d (%d job(s))",
        path,
        config.name,
        config.version,
        len(config.jobs),
    )
    return config


def _apply_default_group(data: dict[str, Any]) -> None:
    """Defaults each consumer's `group` to the workflow name when absent.

    Applied before Pydantic sees the data — this is a JSON preprocessing
    rule, not a per-field validation, so it doesn't belong in the model
    itself. Malformed shapes (e.g. `consumers` not a list) are left alone;
    Pydantic will report those directly.

    Args:
        data: The raw parsed JSON, mutated in place.
    """
    name = data.get("name")
    if not isinstance(name, str):
        return
    consumers = data.get("consumers")
    if not isinstance(consumers, list):
        return
    for consumer in consumers:
        if isinstance(consumer, dict) and not consumer.get("group"):
            consumer["group"] = name


def _format_validation_error(path: Path, exc: ValidationError) -> str:
    """Formats a pydantic ValidationError into one ConfigError-ready string.

    Args:
        path: The config file that failed to validate.
        exc: The validation error to format.

    Returns:
        A single-line, semicolon-joined summary of every error.
    """
    parts = []
    for error in exc.errors():
        loc = ".".join(str(p) for p in error["loc"])
        parts.append(f"{loc}: {error['msg']}" if loc else error["msg"])
    return f"{path}: " + "; ".join(parts)


# -- structural validation (post-parse, cross-field / cross-file / filesystem) --


def _validate_workflow(config: WorkflowConfig, ctx: str) -> None:
    """Runs every cross-field structural check on one parsed config.

    Args:
        config: The parsed config to validate.
        ctx: Human-readable file/context label for error messages.

    Raises:
        ConfigError: If any check fails.
    """
    consumer_by_name = {c.name: c for c in config.consumers}
    producer_by_name = {p.name: p for p in config.producers}
    _check_name_references(config, consumer_by_name, producer_by_name, ctx)
    _check_entry_consumer(config, ctx)
    _check_unique_consumer_topics(config, consumer_by_name, ctx)
    _check_no_cycles(config, ctx)


def _check_name_references(
    config: WorkflowConfig,
    consumer_by_name: dict[str, ConsumerSpec],
    producer_by_name: dict[str, ProducerSpec],
    ctx: str,
) -> None:
    """Checks every job.consumer / producer route name against declared specs.

    Args:
        config: The parsed config to validate.
        consumer_by_name: The config's consumers, keyed by name.
        producer_by_name: The config's producers, keyed by name.
        ctx: Human-readable file/context label for error messages.

    Raises:
        ConfigError: If a job references an undeclared consumer or producer.
    """
    for job in config.jobs:
        if job.consumer not in consumer_by_name:
            raise ConfigError(
                f"{ctx}: job {job.name!r} consumer {job.consumer!r} "
                "does not match any declared consumer name"
            )
        for route in job.producer:
            if route.name not in producer_by_name:
                raise ConfigError(
                    f"{ctx}: job {job.name!r} producer route {route.name!r} "
                    "does not match any declared producer name"
                )


def _check_entry_consumer(config: WorkflowConfig, ctx: str) -> None:
    """Checks that entry_consumer, if set, names a consumer some job uses.

    Args:
        config: The parsed config to validate.
        ctx: Human-readable file/context label for error messages.

    Raises:
        ConfigError: If entry_consumer doesn't match any job's consumer.
    """
    job_consumer_names = {job.consumer for job in config.jobs}
    if config.entry_consumer is not None and config.entry_consumer not in job_consumer_names:
        raise ConfigError(
            f"{ctx}: entry_consumer {config.entry_consumer!r} does not match any job's consumer"
        )


def _check_unique_consumer_topics(
    config: WorkflowConfig, consumer_by_name: dict[str, ConsumerSpec], ctx: str
) -> None:
    """Checks that no two jobs in this config consume the same topic.

    Args:
        config: The parsed config to validate.
        consumer_by_name: The config's consumers, keyed by name.
        ctx: Human-readable file/context label for error messages.

    Raises:
        ConfigError: If two jobs share an input topic.
    """
    seen: dict[str, str] = {}
    for job in config.jobs:
        topic = consumer_by_name[job.consumer].topic
        if topic in seen:
            raise ConfigError(
                f"{ctx}: topic {topic!r} is consumed by both {seen[topic]!r} and {job.name!r}"
            )
        seen[topic] = job.name


def _check_no_cycles(config: WorkflowConfig, ctx: str) -> None:
    """Checks the job graph (input topic -> possible output topics) for cycles.

    Conservative for multi-route jobs: a job with multiple conditional
    routes gets an edge to EVERY declared route, since which one fires
    depends on runtime data unavailable at validation time — if any
    possible route would cycle, this rejects the config.

    Args:
        config: The parsed config to validate.
        ctx: Human-readable file/context label for error messages.

    Raises:
        ConfigError: If any possible route forms a cycle.
    """
    consumer_by_name = {c.name: c for c in config.consumers}
    producer_by_name = {p.name: p for p in config.producers}

    edges: dict[str, set[str]] = {}
    for job in config.jobs:
        consumer_topic = consumer_by_name[job.consumer].topic
        for route in job.producer:
            producer_topic = producer_by_name[route.name].topic
            edges.setdefault(consumer_topic, set()).add(producer_topic)

    WHITE, GRAY, BLACK = 0, 1, 2
    color: dict[str, int] = {}

    def visit(topic: str) -> None:
        color[topic] = GRAY
        for succ in edges.get(topic, ()):
            state = color.get(succ, WHITE)
            if state == GRAY:
                raise ConfigError(f"{ctx}: cycle detected through topic {succ!r}")
            if state == WHITE:
                visit(succ)
        color[topic] = BLACK

    for topic in edges:
        if color.get(topic, WHITE) == WHITE:
            visit(topic)


def _check_cross_file_consumers(configs: list[WorkflowConfig]) -> None:
    """Checks that no topic is a job consumer in more than one workflow.

    Args:
        configs: Every workflow config loaded from one directory.

    Raises:
        ConfigError: If the same topic is a job consumer in two different
            workflows.
    """
    owners: dict[str, str] = {}
    for config in configs:
        consumer_by_name = {c.name: c for c in config.consumers}
        for job in config.jobs:
            topic = consumer_by_name[job.consumer].topic
            owner = owners.get(topic)
            if owner is not None and owner != config.name:
                raise ConfigError(
                    f"topic {topic!r} is a job consumer in both "
                    f"workflow {owner!r} and workflow {config.name!r}"
                )
            owners[topic] = config.name


def _validate_avro_schemas(config: WorkflowConfig, base_dir: Path, ctx: str) -> None:
    """Validates every schema_avro_value/schema_avro_key reference in a config.

    Args:
        config: The parsed config whose consumer/producer schema refs to
            validate.
        base_dir: Directory schema paths are resolved relative to (the
            config file's own directory).
        ctx: Human-readable file/context label for error messages.

    Raises:
        ConfigError: If any referenced schema file is missing or invalid.
    """
    for i, consumer in enumerate(config.consumers):
        _validate_avro_schema(
            consumer.schema_avro_value, base_dir, f"{ctx} consumers[{i}] schema_avro_value"
        )
        _validate_avro_schema(
            consumer.schema_avro_key, base_dir, f"{ctx} consumers[{i}] schema_avro_key"
        )
    for i, producer in enumerate(config.producers):
        _validate_avro_schema(
            producer.schema_avro_value, base_dir, f"{ctx} producers[{i}] schema_avro_value"
        )
        _validate_avro_schema(
            producer.schema_avro_key, base_dir, f"{ctx} producers[{i}] schema_avro_key"
        )


def _validate_avro_schema(schema_ref: str | None, base_dir: Path, ctx: str) -> None:
    """Validates one schema_avro_value/schema_avro_key reference, if present.

    Resolved relative to the *config file's own directory*, so a workflow
    config and its schema/ subfolder stay portable regardless of the
    working directory `mira-flow` is run from.

    This validates schema *shape* only — message payloads are still plain
    JSON in v1 (§2/§8); nothing here encodes/decodes Avro binary or talks to
    a Schema Registry.

    Args:
        schema_ref: Path to the schema file, relative to `base_dir`, or
            None to skip validation entirely.
        base_dir: Directory `schema_ref` is resolved relative to.
        ctx: Human-readable file/context label for error messages.

    Raises:
        ConfigError: If `schema_ref` doesn't point to a file, isn't valid
            JSON, or isn't a structurally valid Avro schema.
    """
    if schema_ref is None:
        return
    schema_path = base_dir / schema_ref
    if not schema_path.is_file():
        raise ConfigError(f"{ctx}: avro schema file not found: {schema_path}")
    try:
        raw_schema = json.loads(schema_path.read_text())
    except json.JSONDecodeError as exc:
        raise ConfigError(f"{ctx}: avro schema {schema_path} is not valid JSON: {exc}") from exc
    try:
        # named_schemas={}: parse each schema in isolation, not against
        # fastavro's default global registry — otherwise two unrelated
        # schema files both naming a record e.g. "Value" would collide.
        fastavro.parse_schema(raw_schema, named_schemas={})
    except Exception as exc:  # noqa: BLE001 - fastavro doesn't expose one stable error type
        raise ConfigError(f"{ctx}: invalid avro schema {schema_path}: {exc}") from exc
