# api_server — HTTP API for a workflow UI

A [FastAPI](https://fastapi.tiangolo.com/) app over the same ledger and
workflow configs the `orchestrator` uses, for building an Airflow-style
dashboard: browse workflows, trigger/cancel/browse runs, browse a run's
tasks, retry a failed one.

## Run it

```bash
uv run uvicorn mira_flow.api_server.app:app --reload --port 8001
```

Broker address (`KAFKA_BOOTSTRAP_SERVERS`), auth (`KAFKA_SASL_*`/`KAFKA_SSL_*`,
see the root README), and database (`DATABASE_URL`) come from the same env
vars/`.env` as everything else. `WORKFLOWS_DIR` (default `workflows`) picks
which directory of JSON configs to serve — loaded once at startup, so a
config file added or changed on disk needs a restart to show up.

## Endpoints

| Method | Path | |
|---|---|---|
| GET | `/health` | Liveness — process is up, nothing else checked |
| GET | `/ready` | Readiness — DB reachable (503 otherwise) |
| GET | `/workflows` | Every configured workflow (from JSON, not the ledger — shows up even with zero runs) |
| GET | `/workflows/{name}` | One workflow's jobs, topics, retry policy |
| GET | `/workflows/{name}/grid` | One page of runs (`?limit=`/`?offset=`, newest first) with each task's latest attempt, plus `has_more` — a whole grid view in one request |
| POST | `/workflows/{name}/trigger` | Start a new run — `{"idempotency_key": ..., "input": {...}}`; 409 if no `entry_consumer` |
| GET | `/runs` | workflow_runs, filterable by `?workflow_name=`/`?status=`, paginated (`?limit=`/`?offset=`) |
| GET | `/runs/{run_id}` | One run, with its full input/output/error |
| POST | `/runs/{run_id}/cancel` | Cancel a PENDING/RUNNING run (409 if already terminal) |
| GET | `/runs/{run_id}/tasks` | Every task_run attempt within a run |
| GET | `/runs/{run_id}/tasks/{task_run_id}` | One attempt, with its full input/output/error |
| POST | `/runs/{run_id}/tasks/{task_run_id}/retry` | Retry a FAILED attempt (409 if not FAILED, or its run is CANCELLED) |

Interactive docs at `/docs` (Swagger UI) once running.

## How retry works

There's no scheduler polling the ledger for retryable rows the way Airflow
has — task retries normally happen entirely inside a `JobWorker`'s own
in-process retry loop while it's handling a message. Once a `task_run` is
FAILED (terminal, retries exhausted), nothing re-processes it on its own.

So `POST .../retry` doesn't touch the DB at all. It republishes the failed
attempt's stored `input` to its job's input topic, with
`Message.force_retry=True`. The orchestrator's already-running `JobWorker`
picks it up like any other message — except `force_retry=True` tells
`WorkflowRepository.start_task_run` to bypass its redelivery dedup (which
would otherwise treat "last attempt is terminal" as "Kafka redelivered an
already-processed message" and silently skip it) and also revive the parent
`workflow_run` from FAILED back to RUNNING. Everything downstream — the new
attempt row, the handler call, success/failure — goes through the exact
same path a first attempt would.

This means retry requires an `orchestrator` process actually running and
subscribed to that topic; this API only publishes the message.

## How cancel works

`POST /runs/{run_id}/cancel` only marks the ledger — there's no live signal
channel to a `JobWorker` that might already be mid-attempt, so this can't
interrupt one. It does have real effect on everything *after* that:
`WorkflowRepository.start_task_run` refuses to start a new attempt once its
run is CANCELLED (even via a force-retry), and a straggling worker that
finishes its in-flight attempt after cancellation can't resurrect the run
back to SUCCEEDED/FAILED — only its own task_run row is still recorded.

## Known limitations (v1)

- **No auth, permissive CORS.** Meant for local/trusted-network use behind
  your own gateway/auth layer — lock both down before exposing this
  anywhere else.
- **Configs loaded once at startup.** A workflow added or changed on disk
  needs a process restart to appear.
- **Retry needs the *current* config to still have that job.** If a
  workflow's config changed since a run started (job renamed/removed), its
  failed tasks can no longer be resolved to an input topic — retry returns
  409 rather than guessing.
- **Trigger/retry don't validate against Avro schemas.** `orchestrator`/
  `core` validate a topic's payload against its configured
  `schema_avro_value` on every publish and consume (see the root README);
  this API's `trigger`/`retry` publish through a broker built without those
  schemas, so a malformed payload here isn't rejected until (if) an
  `orchestrator` worker consumes it and skips it as a poison pill — not
  immediately, at request time.
- **Cancel can't interrupt an in-flight attempt** — see above.
