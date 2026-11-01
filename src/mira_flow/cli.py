"""Typer CLI: init-db, run, trigger."""

from __future__ import annotations

import importlib
import json
import logging
import os
import signal
import sys
from pathlib import Path
from typing import Any

import typer

from mira_flow.core import WorkflowManager
from mira_flow.ledger import WorkflowRepository, init_db, session_scope
from mira_flow.orchestrator import JobBinding, Orchestrator, build_bindings
from mira_flow.settings import settings
from mira_flow.transport import KafkaBroker, Message
from mira_flow.wiring import (
    ConfigError,
    WorkflowConfig,
    default_registry,
    load_avro_schema,
    load_handlers_from_dir,
    load_workflows,
)

app = typer.Typer(help="Mira Flow: a minimal event-driven workflow engine")


@app.callback()
def main(
    log_level: str = typer.Option(
        "INFO", "--log-level", help="Logging level: DEBUG, INFO, WARNING, ERROR, or CRITICAL."
    ),
) -> None:
    """Mira Flow: a minimal event-driven workflow engine."""
    # The installed `mira-flow` console-script is a file path, not `-m`/`-c`
    # — Python sets sys.path[0] to *its* directory (e.g. .../venv/bin) in
    # that case, not the current working directory, so `--import <module>`
    # (a plain importlib.import_module, unlike --handlers-dir's by-path
    # loading) could never resolve a module relative to wherever the user
    # actually runs `mira-flow` from. `python -m`/`-c` don't have this
    # problem (they put '' / CWD on sys.path[0] automatically) — this makes
    # the console-script behave the same way.
    if (cwd := os.getcwd()) not in sys.path:
        sys.path.insert(0, cwd)
    logging.basicConfig(
        level=log_level.upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def _install_sigterm_handler(stoppable: Orchestrator | WorkflowManager) -> None:
    """Translates SIGTERM into a clean `stop()`, for graceful shutdown e.g.
    under Kubernetes (whose pod-termination signal is SIGTERM, not the
    SIGINT that Python already raises as KeyboardInterrupt on Ctrl+C).

    `signal.signal()` only works from the main thread, and installing a
    handler is process-global — both are reasons this belongs at the CLI
    entry point specifically, not inside Orchestrator/WorkflowManager
    (embeddable library classes any thread could run under, and whose
    caller may want to own process signal handling itself) or JobWorker
    (which never runs on the main thread to begin with).

    Args:
        stoppable: The running Orchestrator/WorkflowManager to stop.
    """

    def handler(signum: int, frame: Any) -> None:
        stoppable.stop()

    signal.signal(signal.SIGTERM, handler)


def _merge_producer_params(bindings: list[JobBinding]) -> dict[str, Any]:
    """Flattens every binding's per-topic producer params into one dict.

    KafkaBroker's single shared Producer takes one flat config dict — there's
    one Producer for every topic, so topic identity doesn't factor in here.
    Two declarations agreeing (same key, same value) merge fine; two
    disagreeing on the same key fail fast here, instead of one silently
    clobbering the other at runtime.

    Args:
        bindings: The job bindings whose producer_params to merge.

    Returns:
        One flat confluent-kafka producer config dict.

    Raises:
        ConfigError: If two bindings declare conflicting values for the
            same producer config key.
    """
    merged: dict[str, Any] = {}
    for binding in bindings:
        for params in binding.producer_params.values():
            for key, value in params.items():
                if key in merged and merged[key] != value:
                    raise ConfigError(
                        f"conflicting producer params: {key!r} is {merged[key]!r} "
                        f"in one declaration and {value!r} in another"
                    )
                merged[key] = value
    return merged


def _merge_producer_schemas(bindings: list[JobBinding]) -> dict[str, dict[str, Any]]:
    """Merges every binding's per-topic output Avro schemas into one dict.

    Args:
        bindings: The job bindings whose output_schemas to merge.

    Returns:
        One topic -> parsed Avro schema dict, for `KafkaBroker.producer_schemas`.

    Raises:
        ConfigError: If two bindings declare different schemas for the same
            topic (a config bug — one topic, one schema).
    """
    merged: dict[str, dict[str, Any]] = {}
    for binding in bindings:
        for topic, schema in binding.output_schemas.items():
            if topic in merged and merged[topic] != schema:
                raise ConfigError(f"conflicting avro schemas declared for topic {topic!r}")
            merged[topic] = schema
    return merged


def _load_configs_or_exit(workflows_dir: str) -> list[WorkflowConfig]:
    """Loads workflow configs, exiting with an error message on failure.

    Args:
        workflows_dir: Directory containing the workflow JSON configs.

    Returns:
        The parsed, validated workflow configs.
    """
    try:
        return load_workflows(workflows_dir)
    except ConfigError as exc:
        typer.echo(f"config error: {exc}", err=True)
        raise typer.Exit(1) from exc


def _load_handlers_from_dirs_or_exit(handlers_dirs: list[str]) -> None:
    """Imports every `.py` file under each directory, exiting on failure.

    Args:
        handlers_dirs: Directories to scan for `@handler`-decorated files
            (see `wiring.load_handlers_from_dir`).
    """
    for handlers_dir in handlers_dirs:
        try:
            loaded = load_handlers_from_dir(handlers_dir)
        except ConfigError as exc:
            typer.echo(f"config error: {exc}", err=True)
            raise typer.Exit(1) from exc
        typer.echo(f"loaded {len(loaded)} handler file(s) from {handlers_dir!r}")


def _parse_json_object_or_exit(text: str) -> dict[str, Any]:
    """Parses a JSON object, exiting with an error message on failure.

    Args:
        text: The raw JSON text to parse.

    Returns:
        The parsed JSON object.
    """
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        typer.echo(f"invalid JSON input: {exc}", err=True)
        raise typer.Exit(1) from exc
    if not isinstance(payload, dict):
        typer.echo("input must be a JSON object", err=True)
        raise typer.Exit(1)
    return payload


@app.command("init-db")
def init_db_command() -> None:
    """Create the workflow_run / task_run tables."""
    init_db()
    typer.echo("database initialized")


def _run_single_workflow(config_path: str) -> None:
    """Runs one workflow config with no DB ledger (`core.WorkflowManager`) —
    `run`'s `--workflow` path.

    Args:
        config_path: Path to the workflow's JSON config.
    """
    try:
        manager = WorkflowManager.from_file(config_path)
    except ConfigError as exc:
        typer.echo(f"config error: {exc}", err=True)
        raise typer.Exit(1) from exc
    except KeyError as exc:
        typer.echo(f"startup error: {exc}", err=True)
        raise typer.Exit(1) from exc

    typer.echo(f"running workflow {config_path!r} (no DB ledger)")
    _install_sigterm_handler(manager)
    manager.run_forever()


@app.command("run")
def run_command(
    workflows_dir: str = typer.Option(
        "workflows",
        "--workflows-dir",
        help="directory of workflow configs, ledgered (ignored if --workflow is set)",
    ),
    workflow: str = typer.Option(
        None,
        "--workflow",
        help="run a single workflow config file with no DB ledger (core.WorkflowManager)",
    ),
    import_modules: list[str] = typer.Option([], "--import", help="handler module to import"),
    handlers_dirs: list[str] = typer.Option(
        [], "--handlers-dir", help="directory of handler .py files to auto-register"
    ),
) -> None:
    """Start the orchestrator (--workflows-dir) or a single ledger-free
    workflow (--workflow): import handlers, load workflow(s), run."""
    for module_name in import_modules:
        importlib.import_module(module_name)
    _load_handlers_from_dirs_or_exit(handlers_dirs)

    if workflow is not None:
        _run_single_workflow(workflow)
        return

    configs = _load_configs_or_exit(workflows_dir)
    if not configs:
        typer.echo(f"no workflow configs found in {workflows_dir!r}", err=True)
        raise typer.Exit(1)

    registry = default_registry()
    try:
        bindings = build_bindings(configs, registry, Path(workflows_dir))
    except KeyError as exc:
        typer.echo(f"startup error: {exc}", err=True)
        raise typer.Exit(1) from exc

    consumer_groups = {b.input_topic: b.consumer_group for b in bindings}
    consumer_params = {b.input_topic: b.consumer_params for b in bindings}
    consumer_schemas = {
        b.input_topic: b.input_schema for b in bindings if b.input_schema is not None
    }
    try:
        producer_params = _merge_producer_params(bindings)
        producer_schemas = _merge_producer_schemas(bindings)
    except ConfigError as exc:
        typer.echo(f"config error: {exc}", err=True)
        raise typer.Exit(1) from exc
    broker = KafkaBroker(
        consumer_groups=consumer_groups,
        consumer_params=consumer_params,
        producer_params=producer_params,
        consumer_schemas=consumer_schemas,
        producer_schemas=producer_schemas,
    )

    typer.echo(f"loaded {len(configs)} workflow(s), {len(bindings)} job(s):")
    for config in configs:
        typer.echo(f"  {config.name} (v{config.version})")
    for binding in bindings:
        destinations = (
            ", ".join(sorted({route.topic for route in binding.output_routes}))
            if binding.output_routes
            else "(terminal)"
        )
        entry = " [entry]" if binding.is_entry else ""
        typer.echo(
            f"    {binding.workflow_name}.{binding.task_name}{entry}: "
            f"{binding.input_topic} (group={binding.consumer_group!r}, "
            f"instances={binding.consumer_instances}) -> {destinations}"
        )
    typer.echo(f"bootstrap servers: {settings.kafka_bootstrap_servers!r}")
    typer.echo("starting orchestrator")
    orchestrator = Orchestrator(bindings, broker)
    _install_sigterm_handler(orchestrator)
    try:
        orchestrator.run_forever()
    finally:
        broker.close()


@app.command("trigger")
def trigger_command(
    workflow_name: str,
    idempotency_key: str,
    json_input: str,
    workflows_dir: str = typer.Option("workflows", "--workflows-dir"),
) -> None:
    """Create a workflow_run and publish the first message to entry_consumer's topic (§4a)."""
    payload = _parse_json_object_or_exit(json_input)
    configs = _load_configs_or_exit(workflows_dir)

    config = next((c for c in configs if c.name == workflow_name), None)
    if config is None:
        typer.echo(f"unknown workflow {workflow_name!r}", err=True)
        raise typer.Exit(1)
    if config.entry_consumer is None:
        typer.echo(
            f"workflow {workflow_name!r} has no entry_consumer; "
            "it must be started by an external service publishing directly to its first topic",
            err=True,
        )
        raise typer.Exit(1)

    entry_topic = next(c.topic for c in config.consumers if c.name == config.entry_consumer)

    with session_scope() as session:
        repo = WorkflowRepository(session)
        run, created = repo.get_or_create_workflow_run(
            workflow_name=config.name,
            workflow_version=config.version,
            idempotency_key=idempotency_key,
            input=payload,
        )
        run_id, correlation_id, status = run.id, run.correlation_id, run.status

    if not created and status != "PENDING":
        # Same rule as the API: only a still-PENDING run gets its trigger
        # message re-published (its first publish may have failed).
        typer.echo(
            f"workflow_run {run_id} already exists for this idempotency key "
            f"({status}); not re-publishing"
        )
        return

    consumer_groups = {c.topic: c.group for c in config.consumers}
    producer_spec = next((p for p in config.producers if p.topic == entry_topic), None)
    entry_schema = (
        load_avro_schema(producer_spec.schema_avro_value, Path(workflows_dir))
        if producer_spec is not None
        else None
    )
    broker = KafkaBroker(
        consumer_groups=consumer_groups,
        producer_params=producer_spec.params if producer_spec else None,
        producer_schemas={entry_topic: entry_schema} if entry_schema is not None else None,
    )
    try:
        broker.publish(
            entry_topic,
            Message(
                topic=entry_topic,
                payload=payload,
                workflow_run_id=run_id,
                correlation_id=correlation_id,
            ),
        )
    finally:
        broker.close()

    typer.echo(f"triggered workflow_run {run_id} (correlation_id={correlation_id})")


if __name__ == "__main__":
    app()
