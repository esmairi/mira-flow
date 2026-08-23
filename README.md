# Mira Flow

Minimal event-driven workflow engine in Python 3.13 / uv.

A workflow is a chain of **jobs** connected by **topics** on a message broker:

```
trigger ──▶ topic A ──▶ job 1 ──▶ topic B ──▶ job 2 ──▶ ... ──▶ terminal job ──▶ done
```

Each job consumes a message, runs a registered Python handler, and publishes the
result to its output topic. Every step is recorded in a database ledger
(Airflow-style: one `workflow_run` per chain, one `task_run` per attempt), with
per-job retry on handler failure.

Wiring lives in JSON configs (`workflows/*.json`), transport behind a `Broker`
protocol implemented on an external Kafka broker (Apache Kafka in dev via
`compose.yaml`, any Kafka in prod — no in-memory fallback), status in a DB
ledger via SQLAlchemy — SQLite by default, Postgres also supported (see
[Database](#database) below), schema managed by Alembic — see `packages/
ledger/README.md`). A topic's optional `schema_avro_value` is enforced at
runtime, not just validated at config-parse time: every publish and consume
checks the payload's shape against it (§8, no Avro binary encoding — the
wire format stays plain JSON).

Four packages in one `uv` workspace: `packages/core` bundles every DB-free
module (`transport`, `wiring`, `execution`, `core` itself) into one
installable, ledger-free runtime; `packages/ledger` is just the DB (used by
both `orchestrator` and `api_server`, which otherwise share nothing);
`packages/api_server` is a FastAPI app over the ledger + workflow configs,
for a UI; `.` (root, the full `mira-flow`) adds the ledgered
`orchestrator` and the CLI on top of `core` + `ledger`. `uv sync`/`uv run`/
`uv lock` at the repo root operate on the whole workspace; every module
still imports as `mira_flow.<name>` regardless of which package it
physically lives in.

## Quick start

Requires Docker (local broker + testcontainers).

```bash
docker compose up -d          # Kafka on localhost:9092, kafka-ui on localhost:8080
uv sync
uv run pytest
uv run mira-flow init-db
uv run mira-flow run --workflows-dir workflows --import examples.echo

# from another terminal:
uv run mira-flow trigger <workflow> <idempotency-key> '{"some": "input"}'
```

Handlers can also be picked up from a directory instead of (or alongside)
`--import`, so they don't need to be an importable Python module —
`--handlers-dir path/to/handlers` recursively imports every `.py` file
there (skipping any file or directory starting with `_`, e.g.
`__pycache__`); each file just needs `@handler("name")`-decorated
functions, same as `examples/echo.py`. `run` accepts it repeatably, in
either of its two modes:

```bash
uv run mira-flow run --workflows-dir workflows --handlers-dir handlers
```

Browse topics/messages at [localhost:8080](http://localhost:8080) (kafka-ui).

See `src/mira_flow/README.md` for the full CLI command reference, including
`run`'s ledger-free `--workflow` mode (no DB needed — good for a quick
local check of one workflow).

### Dev container

`docker compose up -d app` starts a glibc dev shell (`Dockerfile`) with the
project installed via `uv sync`, on the same Docker network as `kafka`. Useful
if your host can't build `confluent-kafka` (e.g. musl/Alpine — no prebuilt
wheel, needs `librdkafka` + a compiler the host may not have):

```bash
docker compose up -d
docker compose exec app bash
# inside the container:
export KAFKA_BOOTSTRAP_SERVERS=kafka:19092
uv run mira-flow init-db
uv run mira-flow run --workflows-dir workflows --import examples.echo
```

The repo is bind-mounted (`.:/app`), so edits on the host apply immediately —
no rebuild needed unless `pyproject.toml`/`uv.lock` change. The venv itself
lives in a named volume (`/opt/venv`, outside the bind mount) so it survives
container restarts and isn't wiped by the mount.

Note: `uv run pytest`'s broker-backed tests (`transport/`, `orchestrator/`, `core/`)
use `testcontainers` to spin up their own ephemeral broker — that needs a
Docker socket, which this dev container does not have mounted. Run those from
a real Docker host instead; the `app` container is for interactive dev /
`init-db` / `run` / `trigger` against the `kafka` service above.

Broker address comes from `KAFKA_BOOTSTRAP_SERVERS` (default `localhost:9092`),
database URL from `DATABASE_URL` (default `sqlite:///mira_flow.db`).

### Database

SQLite by default — nothing to install, nothing to run. Postgres is also
supported; the ledger's schema and every query are dialect-agnostic
(SQLAlchemy), so switching is just a `DATABASE_URL` and a driver, not a
different code path.

**Local Postgres, dev container:**

```bash
docker compose --profile postgres up -d postgres   # postgres:16-alpine, localhost:5432
uv sync --extra postgres                            # installs psycopg
export DATABASE_URL=postgresql+psycopg://mira:mira@localhost:5432/mira_flow
uv run alembic -c packages/ledger/alembic.ini upgrade head
uv run mira-flow init-db   # or the alembic command above; not both (see packages/ledger/README.md)
```

From inside the `app` container, use `postgres:5432` instead of
`localhost:5432` (compose network hostname vs. the published host port).

**Prod-image worker against Postgres**, no manual driver/migration steps —
`make worker-postgres` (or `docker compose --profile worker-postgres up
worker-postgres`) brings up `postgres`, runs `migrate-postgres` (Alembic)
to completion, then starts `worker-postgres`, all wired via `depends_on`.
This is a **separate image build** from the plain `worker`/`migrate`
(`Dockerfile`'s `DB_EXTRA` build arg) — only the Postgres-flavored images
carry the `psycopg` driver, so the sqlite path doesn't pay for a driver it
never uses. See `compose.yaml`'s `postgres`/`migrate-postgres`/
`worker-postgres` services for the exact wiring, including how to point at
an external Postgres instead of the compose-managed one.

**Adding a database beyond these two**: one line in `packages/ledger/
pyproject.toml`'s `optional-dependencies` (the driver), and — only if that
dialect needs engine tuning (pool settings, connect args) — one branch in
`packages/ledger/src/mira_flow/ledger/db.py`'s `_engine_kwargs`. No new
package, no new CLI, no orchestrator changes. A missing driver fails fast
with the exact `uv sync --extra ...` to run, instead of a raw
`ModuleNotFoundError` from deep inside SQLAlchemy.

### Kafka auth

No auth by default (PLAINTEXT, as in `compose.yaml`). For a real cluster,
set whichever of these apply — never in `workflows/*.json`, which is not a
place for secrets:

| Env var | confluent-kafka key |
|---|---|
| `KAFKA_SECURITY_PROTOCOL` | `security.protocol` (e.g. `SASL_SSL`) |
| `KAFKA_SASL_MECHANISM` | `sasl.mechanism` (e.g. `SCRAM-SHA-512`) |
| `KAFKA_SASL_USERNAME` / `KAFKA_SASL_PASSWORD` | `sasl.username` / `sasl.password` |
| `KAFKA_SSL_CA_LOCATION` | `ssl.ca.location` |
| `KAFKA_SSL_CERTIFICATE_LOCATION` / `KAFKA_SSL_KEY_LOCATION` | mTLS client cert/key |
| `KAFKA_SSL_KEY_PASSWORD` | `ssl.key.password` |

Kubernetes-friendly by design: set these directly as env vars (e.g. via
`secretKeyRef`), or — safer, since they then never show up in `env`/`kubectl
describe pod` — mount a Secret as a volume and point `KAFKA_SECRETS_DIR` at
it (one file per value, named after the env var lowercased, e.g.
`kafka_sasl_password`). Env vars win if both are set. See
`packages/core/src/mira_flow/settings.py` for the full mechanism —
whatever's set here applies to every topic; an individual topic's `params`
(§2 of the workflow config) can still override it if it genuinely needs
different credentials.

### API server

`packages/api_server` is a small FastAPI app for a UI — browse workflows,
trigger/cancel/browse runs, browse tasks, retry a failed one, `/health` +
`/ready` probes (Airflow-style):

```bash
uv run uvicorn mira_flow.api_server.app:app --reload --port 8001
```

Interactive docs at `/docs`. See `packages/api_server/src/mira_flow/
api_server/README.md` for the full endpoint list, how retry/cancel actually
work (they publish to / mark the ledger — an `orchestrator` still needs to
be running to act on either), and known v1 limitations (no auth, permissive
CORS).
