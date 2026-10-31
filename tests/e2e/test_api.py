"""End-to-end API tests: HTTP request -> api_server -> real Kafka -> real
Orchestrator/JobWorkers -> real Postgres ledger -> back out through the API.

See conftest.py for the stack and the generated test workflows.
"""

from __future__ import annotations

import uuid
from typing import Any

from fastapi.testclient import TestClient
from sqlalchemy import text

from tests.e2e.conftest import (
    BLOCKING,
    EXTERNAL,
    FLAKY,
    LINEAR,
    HandlerControls,
    wait_for,
    wait_for_run_status,
)


def _trigger(
    client: TestClient, workflow: str, payload: dict[str, Any], key: str | None = None
) -> dict[str, Any]:
    response = client.post(
        f"/workflows/{workflow}/trigger",
        json={"idempotency_key": key or uuid.uuid4().hex, "input": payload},
    )
    assert response.status_code == 200, response.text
    return response.json()


def _tasks(client: TestClient, run_id: str) -> list[dict[str, Any]]:
    response = client.get(f"/runs/{run_id}/tasks")
    assert response.status_code == 200, response.text
    return response.json()


# -- health -------------------------------------------------------------------


def test_health(client: TestClient) -> None:
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_ready_hits_postgres(client: TestClient) -> None:
    from mira_flow.ledger import session_scope

    response = client.get("/ready")
    assert response.status_code == 200
    assert response.json() == {"status": "ready"}
    with session_scope() as session:
        assert session.execute(text("SELECT version()")).scalar_one().startswith("PostgreSQL")


# -- workflows ----------------------------------------------------------------


def test_list_workflows(client: TestClient) -> None:
    response = client.get("/workflows")
    assert response.status_code == 200
    names = {workflow["name"] for workflow in response.json()}
    assert names == {LINEAR, FLAKY, BLOCKING, EXTERNAL}


def test_get_workflow(client: TestClient, suffix: str) -> None:
    response = client.get(f"/workflows/{LINEAR}")
    assert response.status_code == 200
    body = response.json()
    assert body["entry_consumer"] == "raw-consumer"
    jobs = {job["name"]: job for job in body["jobs"]}
    assert jobs["ingest"]["is_entry"] is True
    assert jobs["ingest"]["input_topic"] == f"e2e-linear-raw-{suffix}"
    assert jobs["ingest"]["producer_routes"] == ["processed-producer"]
    assert jobs["transform"]["is_entry"] is False
    assert jobs["transform"]["producer_routes"] == []


def test_get_unknown_workflow_404(client: TestClient) -> None:
    assert client.get("/workflows/nope").status_code == 404


def test_trigger_unknown_workflow_404(client: TestClient) -> None:
    response = client.post("/workflows/nope/trigger", json={"idempotency_key": "k", "input": {}})
    assert response.status_code == 404


def test_trigger_without_entry_consumer_409(client: TestClient) -> None:
    response = client.post(
        f"/workflows/{EXTERNAL}/trigger", json={"idempotency_key": "k", "input": {}}
    )
    assert response.status_code == 409


def test_trigger_requires_idempotency_key(client: TestClient) -> None:
    response = client.post(f"/workflows/{LINEAR}/trigger", json={"input": {}})
    assert response.status_code == 422


# -- happy path through Kafka ---------------------------------------------------


def test_trigger_runs_linear_pipeline_to_success(client: TestClient, suffix: str) -> None:
    triggered = _trigger(client, LINEAR, {"text": "hello kafka"})
    assert triggered["topic"] == f"e2e-linear-raw-{suffix}"
    run_id = triggered["workflow_run_id"]

    run = wait_for_run_status(client, run_id, {"SUCCEEDED", "FAILED"})
    assert run["status"] == "SUCCEEDED", run
    assert run["workflow_name"] == LINEAR
    assert run["input"] == {"text": "hello kafka"}
    assert run["output"] == {"text": "hello kafka", "length": 11, "upper": "HELLO KAFKA"}
    assert run["correlation_id"] == triggered["correlation_id"]
    assert run["started_at"] is not None
    assert run["ended_at"] is not None

    tasks = _tasks(client, run_id)
    assert [(t["task_name"], t["attempt"], t["status"]) for t in tasks] == [
        ("ingest", 1, "SUCCEEDED"),
        ("transform", 1, "SUCCEEDED"),
    ]
    assert all(t["correlation_id"] == run["correlation_id"] for t in tasks)

    transform = next(t for t in tasks if t["task_name"] == "transform")
    detail = client.get(f"/runs/{run_id}/tasks/{transform['id']}")
    assert detail.status_code == 200
    assert detail.json()["input"] == {"text": "hello kafka", "length": 11}
    assert detail.json()["output"] == run["output"]


def test_trigger_is_idempotent(client: TestClient) -> None:
    key = uuid.uuid4().hex
    first = _trigger(client, LINEAR, {"text": "once"}, key=key)
    second = _trigger(client, LINEAR, {"text": "once"}, key=key)
    assert first["workflow_run_id"] == second["workflow_run_id"]

    run_id = first["workflow_run_id"]
    wait_for_run_status(client, run_id, {"SUCCEEDED"})
    # The duplicate trigger message is a redelivery of already-terminal
    # tasks: the ledger must still hold exactly one attempt per task.
    tasks = _tasks(client, run_id)
    assert sorted((t["task_name"], t["attempt"]) for t in tasks) == [
        ("ingest", 1),
        ("transform", 1),
    ]


def test_external_publish_creates_implicit_run(client: TestClient, external_publish) -> None:
    marker = uuid.uuid4().hex
    external_publish({"text": marker})

    def find_run() -> dict[str, Any] | None:
        runs = client.get("/runs", params={"workflow_name": LINEAR, "limit": 200}).json()
        for summary in runs:
            run = client.get(f"/runs/{summary['id']}").json()
            if run["input"] == {"text": marker}:
                return run
        return None

    run = wait_for(find_run, what="implicit run for the external message")
    run = wait_for_run_status(client, run["id"], {"SUCCEEDED"})
    assert run["idempotency_key"] is None
    assert run["output"] == {"text": marker, "length": len(marker), "upper": marker.upper()}


# -- runs listing / lookup ---------------------------------------------------------


def test_list_runs_filters_and_paginates(client: TestClient) -> None:
    run_ids = [_trigger(client, LINEAR, {"text": f"list-{i}"})["workflow_run_id"] for i in range(3)]
    for run_id in run_ids:
        wait_for_run_status(client, run_id, {"SUCCEEDED"})

    runs = client.get("/runs", params={"workflow_name": LINEAR, "limit": 200}).json()
    listed = [run["id"] for run in runs]
    assert set(run_ids) <= set(listed)
    assert all(run["workflow_name"] == LINEAR for run in runs)
    created = [run["created_at"] for run in runs]
    assert created == sorted(created, reverse=True)

    succeeded = client.get(
        "/runs", params={"workflow_name": LINEAR, "status": "SUCCEEDED", "limit": 200}
    ).json()
    assert all(run["status"] == "SUCCEEDED" for run in succeeded)
    assert set(run_ids) <= {run["id"] for run in succeeded}

    page1 = client.get("/runs", params={"workflow_name": LINEAR, "limit": 2}).json()
    page2 = client.get("/runs", params={"workflow_name": LINEAR, "limit": 2, "offset": 2}).json()
    assert len(page1) == 2
    assert not {r["id"] for r in page1} & {r["id"] for r in page2}

    assert client.get("/runs", params={"status": "NOPE"}).json() == []


def test_list_runs_validates_limit(client: TestClient) -> None:
    assert client.get("/runs", params={"limit": 0}).status_code == 422
    assert client.get("/runs", params={"limit": 201}).status_code == 422
    assert client.get("/runs", params={"offset": -1}).status_code == 422


def test_unknown_run_404s(client: TestClient) -> None:
    missing = str(uuid.uuid4())
    assert client.get(f"/runs/{missing}").status_code == 404
    assert client.post(f"/runs/{missing}/cancel").status_code == 404
    assert client.get(f"/runs/{missing}/tasks").status_code == 404
    assert client.get(f"/runs/{missing}/tasks/{missing}").status_code == 404
    assert client.post(f"/runs/{missing}/tasks/{missing}/retry").status_code == 404


def test_task_from_another_run_404(client: TestClient) -> None:
    run_a = _trigger(client, LINEAR, {"text": "a"})["workflow_run_id"]
    run_b = _trigger(client, LINEAR, {"text": "b"})["workflow_run_id"]
    wait_for_run_status(client, run_a, {"SUCCEEDED"})
    wait_for_run_status(client, run_b, {"SUCCEEDED"})
    task_of_a = _tasks(client, run_a)[0]["id"]
    assert client.get(f"/runs/{run_b}/tasks/{task_of_a}").status_code == 404


# -- failure, retry, cancel ---------------------------------------------------------


def test_failed_task_retried_via_api(client: TestClient, controls: HandlerControls) -> None:
    controls.flaky_should_fail = True
    run_id = _trigger(client, FLAKY, {"n": 1})["workflow_run_id"]

    run = wait_for_run_status(client, run_id, {"FAILED", "SUCCEEDED"})
    assert run["status"] == "FAILED", run
    assert "simulated failure" in str(run["error"])

    # retries=1: one UP_FOR_RETRY attempt, then a terminal FAILED one.
    tasks = _tasks(client, run_id)
    assert [(t["attempt"], t["status"]) for t in tasks] == [(1, "UP_FOR_RETRY"), (2, "FAILED")]
    failed_task = tasks[-1]

    # Only FAILED attempts are retryable.
    assert client.post(f"/runs/{run_id}/tasks/{tasks[0]['id']}/retry").status_code == 409

    controls.flaky_should_fail = False
    response = client.post(f"/runs/{run_id}/tasks/{failed_task['id']}/retry")
    assert response.status_code == 200, response.text
    retry = response.json()
    assert retry["task_run_id"] == failed_task["id"]
    assert retry["workflow_run_id"] == run_id

    run = wait_for_run_status(client, run_id, {"SUCCEEDED"})
    assert run["output"] == {"recovered": True, "n": 1}
    tasks = _tasks(client, run_id)
    assert [(t["attempt"], t["status"]) for t in tasks] == [
        (1, "UP_FOR_RETRY"),
        (2, "FAILED"),
        (3, "SUCCEEDED"),
    ]

    # A SUCCEEDED attempt is never retryable.
    assert client.post(f"/runs/{run_id}/tasks/{tasks[-1]['id']}/retry").status_code == 409


def test_cancel_running_run(client: TestClient, controls: HandlerControls) -> None:
    controls.blocking_started.clear()
    controls.release_blocking.clear()
    run_id = _trigger(client, BLOCKING, {})["workflow_run_id"]

    wait_for(controls.blocking_started.is_set, what="blocking handler to start")
    assert client.get(f"/runs/{run_id}").json()["status"] == "RUNNING"

    response = client.post(f"/runs/{run_id}/cancel")
    assert response.status_code == 200, response.text
    assert response.json()["status"] == "CANCELLED"
    assert response.json()["ended_at"] is not None

    # Let the in-flight handler finish: its success must not resurrect the run.
    controls.release_blocking.set()
    wait_for(
        lambda: _tasks(client, run_id)[0]["status"] == "SUCCEEDED",
        what="blocking task to finish",
    )
    assert client.get(f"/runs/{run_id}").json()["status"] == "CANCELLED"

    # Already terminal: a second cancel is rejected.
    assert client.post(f"/runs/{run_id}/cancel").status_code == 409


def test_cancel_finished_run_409(client: TestClient) -> None:
    run_id = _trigger(client, LINEAR, {"text": "done"})["workflow_run_id"]
    wait_for_run_status(client, run_id, {"SUCCEEDED"})
    assert client.post(f"/runs/{run_id}/cancel").status_code == 409
