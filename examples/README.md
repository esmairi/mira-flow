# Examples

Each case below is self-contained: its own `workflows/` (one config),
`handlers/` directory, and `trigger.py` script, runnable independently with
`mira-flow run --workflow` — no DB ledger, no `--workflows-dir` scanning.
Requires `docker compose up -d` (Kafka on `localhost:9092`) and `uv sync`
first.

| Case | Demonstrates |
|---|---|
| [`linear-pipeline/`](linear-pipeline) | A strictly sequential two-job chain: `entry_consumer` -> job -> job -> terminal (`producer: null`), bare-string producer route; also headers — `ingest` sets one via `HandlerResult`, `transform` reads it via `message.headers` |
| [`conditional-routing/`](conditional-routing) | One job routing to 4 topics: a plain `Comparison`, `AnyOf`, `AllOf`, and a catch-all default route (no `when`, must be last) |
| [`avro-schema/`](avro-schema) | `schema_avro_value`/`schema_avro_key` on both consumer and producer, and the two different runtime-enforcement failure modes (poison-pill skip vs. retry) |
| [`external-trigger/`](external-trigger) | A workflow with no `entry_consumer` — started by an external publisher, not `mira-flow trigger` |
| [`parallel-instances/`](parallel-instances) | `instances: 3` on a consumer — multiple JobWorker threads sharing one consumer group |
| [`retries/`](retries) | `retries` + `retry_delay_seconds` — a handler that fails a few times before succeeding |

## Running a case

Start the workflow, then in another terminal, publish to it with
`trigger.py` (`run` itself never publishes anything):

```bash
uv run mira-flow run --workflow examples/linear-pipeline/workflows/linear-pipeline.json \
    --handlers-dir examples/linear-pipeline/handlers

# in another terminal:
uv run python examples/linear-pipeline/trigger.py
```

Every case's `trigger.py` is a standalone producer (`KafkaBroker` directly,
not the CLI) — it works against *any* running instance of the workflow
(`run --workflow`, `run --workflows-dir`, or the docker compose `worker`
service), needs no DB ledger, and defaults every field it can to random
values so repeat runs need no flags:

| Script | What it sends |
|---|---|
| `linear-pipeline/trigger.py [text]` | One message with random `text` (or a positional override) |
| `conditional-routing/trigger.py [route\|all]` | One random-but-in-bounds payload per route (default: all 4, one per route) — pass `flagged`/`high`/`medium`/`low` for just one |
| `avro-schema/trigger.py [--invalid] [--order-id ID] [--amount N]` | A schema-conforming order with random `order_id`/`amount`; `--invalid` omits `amount` to trigger the consumer-side poison-pill skip |
| `external-trigger/trigger.py [--payload JSON]` | A random signup-style event — this is the *only* way to start this workflow (no `entry_consumer`) |
| `parallel-instances/trigger.py [--count N]` | `N` (default 10) tasks with random `task_id`s, to watch the 3 instances split the work |
| `retries/trigger.py` | One task, to watch `flaky_handler` fail twice then succeed on attempt 3 |

Every script also accepts `--bootstrap-servers` to override
`KAFKA_BOOTSTRAP_SERVERS` (e.g. `--bootstrap-servers kafka:19092` when
running inside the `app`/`worker` containers instead of on the host).

`external-trigger` and `parallel-instances` (without a pre-partitioned
topic) can alternatively be triggered via kafka-ui at
[localhost:8080](http://localhost:8080), producing directly to the relevant
topic. See each case's `handlers/handlers.py` docstring for specifics.

`echo.py` (this directory, not a subfolder) is the minimal smoke-test
handler referenced by the top-level README's quick start
(`--import examples.echo`) — kept flat since it's imported as a module,
not loaded via `--handlers-dir`.
