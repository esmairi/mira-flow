"""End-to-end fixtures: real Kafka + Postgres (tests/e2e/compose.yaml), the
real Orchestrator running in a background thread, and the real api_server
app driven over HTTP via FastAPI's TestClient.

Skipped unless MIRA_FLOW_E2E=1 — `make test-e2e` sets it, along with
KAFKA_BOOTSTRAP_SERVERS / DATABASE_URL pointing at the compose stack. Both
are read by module-level settings objects at import time, which is why
nothing from mira_flow is imported at module scope here.

Every session generates its workflow configs with a unique topic/group
suffix, so reruns against a long-lived stack never see stale messages.
"""

from __future__ import annotations

import json
import os
import threading
import time
import uuid
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest

if os.environ.get("MIRA_FLOW_E2E") != "1":
    pytest.skip(
        "e2e tests need Kafka + Postgres: run `make test-e2e` (or set MIRA_FLOW_E2E=1)",
        allow_module_level=True,
    )

from fastapi.testclient import TestClient  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[2]
ALEMBIC_INI = REPO_ROOT / "packages" / "ledger" / "alembic.ini"

# Workflow names (stable, test-facing) -> generated configs below.
LINEAR = "e2e_linear"
FLAKY = "e2e_flaky"
BLOCKING = "e2e_blocking"
EXTERNAL = "e2e_external"


class HandlerControls:
    """Knobs the tests flip to steer handler behavior at runtime.

    Attributes:
        flaky_should_fail: While True, `e2e_flaky_handler` always raises.
        release_blocking: `e2e_blocking_handler` waits on this before returning.
        blocking_started: Set by `e2e_blocking_handler` once it's running.
    """

    def __init__(self) -> None:
        self.flaky_should_fail = True
        self.release_blocking = threading.Event()
        self.blocking_started = threading.Event()


def _workflow_configs(suffix: str) -> list[dict[str, Any]]:
    """Builds the test workflow configs, with session-unique topics/groups.

    Args:
        suffix: Appended to every topic and consumer group name.

    Returns:
        One JSON-serializable config per test workflow.
    """

    def topic(name: str) -> str:
        return f"e2e-{name}-{suffix}"

    def group(name: str) -> str:
        return f"{name}-{suffix}"

    return [
        {
            "name": LINEAR,
            "version": 1,
            "entry_consumer": "raw-consumer",
            "consumers": [
                {"name": "raw-consumer", "topic": topic("linear-raw"), "group": group(LINEAR)},
                {
                    "name": "processed-consumer",
                    "topic": topic("linear-processed"),
                    "group": group(LINEAR),
                },
            ],
            "producers": [{"name": "processed-producer", "topic": topic("linear-processed")}],
            "jobs": [
                {
                    "name": "ingest",
                    "handler": "e2e_ingest_handler",
                    "consumer": "raw-consumer",
                    "producer": "processed-producer",
                },
                {
                    "name": "transform",
                    "handler": "e2e_transform_handler",
                    "consumer": "processed-consumer",
                    "producer": None,
                },
            ],
        },
        {
            "name": FLAKY,
            "version": 1,
            "entry_consumer": "tasks-consumer",
            "consumers": [
                {"name": "tasks-consumer", "topic": topic("flaky"), "group": group(FLAKY)}
            ],
            "producers": [],
            "jobs": [
                {
                    "name": "flaky_task",
                    "handler": "e2e_flaky_handler",
                    "consumer": "tasks-consumer",
                    "producer": None,
                    "retries": 1,
                    "retry_delay_seconds": 0,
                }
            ],
        },
        {
            "name": BLOCKING,
            "version": 1,
            "entry_consumer": "tasks-consumer",
            "consumers": [
                {"name": "tasks-consumer", "topic": topic("blocking"), "group": group(BLOCKING)}
            ],
            "producers": [],
            "jobs": [
                {
                    "name": "blocking_task",
                    "handler": "e2e_blocking_handler",
                    "consumer": "tasks-consumer",
                    "producer": None,
                }
            ],
        },
        {
            # No entry_consumer: the trigger endpoint must refuse it (409).
            "name": EXTERNAL,
            "version": 1,
            "consumers": [
                {"name": "events-consumer", "topic": topic("external"), "group": group(EXTERNAL)}
            ],
            "producers": [],
            "jobs": [
                {
                    "name": "handle_event",
                    "handler": "e2e_transform_handler",
                    "consumer": "events-consumer",
                    "producer": None,
                }
            ],
        },
    ]


def _build_registry(controls: HandlerControls) -> Any:
    """Registers the test handlers on a fresh (non-global) registry.

    Args:
        controls: Runtime knobs the handlers read.

    Returns:
        A HandlerRegistry with every handler the test workflows reference.
    """
    from mira_flow.transport import Message
    from mira_flow.wiring import HandlerRegistry, HandlerResult

    registry = HandlerRegistry()

    @registry.handler("e2e_ingest_handler")
    def ingest(message: Message, params: dict[str, Any]) -> HandlerResult:
        text = message.payload.get("text", "")
        return HandlerResult(payload={"text": text, "length": len(text)}, headers={"source": "e2e"})

    @registry.handler("e2e_transform_handler")
    def transform(message: Message, params: dict[str, Any]) -> dict[str, Any]:
        return {**message.payload, "upper": str(message.payload.get("text", "")).upper()}

    @registry.handler("e2e_flaky_handler")
    def flaky(message: Message, params: dict[str, Any]) -> dict[str, Any]:
        if controls.flaky_should_fail:
            raise RuntimeError("e2e: simulated failure")
        return {"recovered": True, **message.payload}

    @registry.handler("e2e_blocking_handler")
    def blocking(message: Message, params: dict[str, Any]) -> dict[str, Any]:
        controls.blocking_started.set()
        controls.release_blocking.wait(timeout=60)
        return {"released": True}

    return registry


@pytest.fixture(scope="session")
def suffix() -> str:
    """A per-session token isolating this run's topics and consumer groups."""
    return uuid.uuid4().hex[:10]


@pytest.fixture(scope="session")
def workflows_dir(tmp_path_factory: pytest.TempPathFactory, suffix: str) -> Path:
    """Writes the generated workflow configs to a temp directory."""
    directory = tmp_path_factory.mktemp("workflows")
    for config in _workflow_configs(suffix):
        (directory / f"{config['name']}.json").write_text(json.dumps(config, indent=2))
    return directory


@pytest.fixture(scope="session")
def migrated_db() -> None:
    """Applies the real Alembic migrations to DATABASE_URL."""
    from alembic import command
    from alembic.config import Config

    from mira_flow.ledger import get_engine

    assert get_engine().dialect.name == "postgresql", (
        "e2e tests must run against Postgres; check DATABASE_URL"
    )
    command.upgrade(Config(str(ALEMBIC_INI)), "head")


@pytest.fixture(scope="session")
def controls() -> HandlerControls:
    """Shared handler knobs for the whole session."""
    return HandlerControls()


@pytest.fixture(scope="session")
def orchestrator(
    migrated_db: None, workflows_dir: Path, controls: HandlerControls
) -> Iterator[Any]:
    """Runs the real ledgered Orchestrator against Kafka in a background thread."""
    from mira_flow.orchestrator import Orchestrator, build_bindings
    from mira_flow.transport import KafkaBroker
    from mira_flow.wiring import load_workflows

    configs = load_workflows(workflows_dir)
    bindings = build_bindings(configs, _build_registry(controls), workflows_dir)
    broker = KafkaBroker(
        consumer_groups={b.input_topic: b.consumer_group for b in bindings},
        consumer_params={b.input_topic: b.consumer_params for b in bindings},
    )
    orch = Orchestrator(bindings, broker, poll_timeout=0.5)
    thread = threading.Thread(target=orch.run_forever, name="e2e-orchestrator", daemon=True)
    thread.start()
    try:
        yield orch
    finally:
        controls.release_blocking.set()
        orch.stop()
        thread.join(timeout=15)
        broker.close()


@pytest.fixture(scope="session")
def client(orchestrator: Any, workflows_dir: Path) -> Iterator[TestClient]:
    """The real api_server app, lifespan included (loads configs, opens a KafkaBroker)."""
    from mira_flow.api_server.app import create_app
    from mira_flow.api_server.settings import api_server_settings

    api_server_settings.workflows_dir = str(workflows_dir)
    with TestClient(create_app()) as test_client:
        yield test_client


@pytest.fixture(scope="session")
def external_publish(suffix: str) -> Iterator[Callable[[dict[str, Any]], str]]:
    """Publishes a bare message (no workflow_run_id) to LINEAR's entry topic,
    like an outside service would, bypassing the trigger endpoint.

    Yields:
        A function taking a payload and returning the published message's ID.
    """
    from mira_flow.transport import KafkaBroker, Message

    broker = KafkaBroker()
    topic = f"e2e-linear-raw-{suffix}"

    def publish(payload: dict[str, Any]) -> str:
        message = Message(topic=topic, payload=payload)
        broker.publish(topic, message)
        return message.id

    try:
        yield publish
    finally:
        broker.close()


def wait_for(
    predicate: Callable[[], Any], *, timeout: float = 60.0, interval: float = 0.25, what: str = ""
) -> Any:
    """Polls `predicate` until it returns something truthy, or fails the test.

    Args:
        predicate: Called repeatedly; its first truthy return is returned.
        timeout: Max seconds to wait.
        interval: Seconds between polls.
        what: Describes the awaited condition, for the failure message.

    Returns:
        The predicate's first truthy return value.
    """
    deadline = time.monotonic() + timeout
    last: Any = None
    while time.monotonic() < deadline:
        last = predicate()
        if last:
            return last
        time.sleep(interval)
    pytest.fail(f"timed out after {timeout}s waiting for {what or predicate!r} (last={last!r})")


def wait_for_run_status(
    client: TestClient, run_id: str, statuses: set[str], timeout: float = 60.0
) -> dict[str, Any]:
    """Polls GET /runs/{run_id} until its status is one of `statuses`.

    Args:
        client: The API client.
        run_id: The run to poll.
        statuses: Acceptable target statuses.
        timeout: Max seconds to wait.

    Returns:
        The run's JSON body once it reached a target status.
    """

    def check() -> dict[str, Any] | None:
        response = client.get(f"/runs/{run_id}")
        assert response.status_code == 200, response.text
        body = response.json()
        return body if body["status"] in statuses else None

    return wait_for(check, timeout=timeout, what=f"run {run_id} to reach {sorted(statuses)}")
