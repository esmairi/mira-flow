# core — one workflow, no ledger

Runs a single workflow config end-to-end against the same Kafka broker as
`orchestrator`, with the same config file format and the same `@handler`
registry — but with **no database at all**. No `workflow_run`/`task_run`
rows, no run history, no `lock_version`, no crash-safe retry resume. Just
consume → handle → produce, per job, in memory.

Reach for this package for a script, a notebook, local dev, or embedding
mira-flow somewhere a DB is friction you don't want yet. Reach for
`orchestrator` instead when you need the run ledger, `UP_FOR_RETRY`/
optimistic-concurrency semantics, redelivery dedup, or serving multiple
workflows from one process.

The `mira-flow-core` distribution bundles every module that doesn't
touch the database: `transport` (Kafka), `wiring` (config parsing/validation),
`execution` (the shared `Worker` protocol + route selection), and `core`
itself (this subpackage) — `pip install mira-flow-core` pulls in all
of it, no `sqlalchemy`/`typer`. Import paths are unaffected: `mira_flow.
transport`, `.wiring`, `.execution`, `.core` all still import exactly as
before regardless of which physical distribution they live in.

## Pieces

- **`consumer/`** — `topic_consumer.py`'s `TopicConsumer` is a thin
  confluent-kafka wrapper scoped to one topic (`consume`/`commit`, manual
  offset commit); `factory.py`'s `ConsumerFactory.create(topic, group,
  params)` builds one per (job, consumer instance) — `ConsumerSpec.instances`.
- **`producer/`** — mirrors `consumer/`: `topic_producer.py`'s
  `TopicProducer` (`publish`, scoped to one topic) and `factory.py`'s
  `ProducerFactory.create(topic, params)`. `manager.py` builds one per unique
  topic a job's producer routes reference (conditional routing, §2), shared
  across that job's instances.
- `consumer/` and `producer/` don't import each other (each has its own small
  `_bootstrap` helper rather than sharing one) — deliberately independent of
  each other and of `transport.KafkaBroker`'s multiplexed-topic design:
  `core` favors one small object per job over one shared broker object. The
  one thing they *do* share is `errors.py`'s `CoreBrokerError`, so callers
  can catch a single exception type regardless of which side raised it.
- `TopicConsumer` creates its topic if it's missing, before subscribing
  (`transport.kafka_support.ensure_topic`, broker-default partitions/
  replication): a consumer subscribed to a not-yet-existing topic otherwise
  only notices it on its next metadata refresh (5 min by default), leaving
  downstream jobs idle. Best effort — a refused creation is just logged;
  set `KAFKA_CREATE_MISSING_TOPICS=false` where topics are provisioned out
  of band.
- Each `TopicProducer` waits for its own delivery report via
  `transport.kafka_support.DeliveryPoller` (one polling thread per
  Producer), never by polling the Producer itself — see its docstring.
- **`job_worker.py`** — `JobWorker` implements `execution.Worker`
  (`handle_one`/`run`/`stop`) — the shape both `core` and `orchestrator`'s
  execution units share, even though the loop bodies aren't literally shared
  code (see `execution/worker.py`'s docstring for why not). Runs one job:
  consume → handler → produce, minus every ledger call. `retries` /
  `retry_delay_seconds` from the job config are honored, but the attempt
  count lives in a local variable, not a `task_run` row; the retry wait uses
  `self._stop_event.wait(delay)` so `stop()` interrupts it immediately
  instead of blocking for the full delay. Conditional routing uses
  `execution.select_route()` on `execution.ResolvedRoute`s — the same
  function `orchestrator` uses, not a separate copy — to pick which declared
  topic to publish to; `NoMatchingRouteError` (from `execution`) if a job has
  routes but none match the handler's output.
- **`manager.py`** — `WorkflowManager.from_file(path)` parses one config via
  `wiring.parse_workflow_file` and builds one `JobWorker` per (job, consumer
  instance) — plus an entry producer if `entry_consumer` is set, resolved to
  that consumer's topic. `trigger(payload)` publishes there; `run_forever()`
  starts one thread per worker; `stop()` shuts them down. `WorkflowManager`
  itself never runs a consume loop — same shape as `orchestrator.Orchestrator`.

## Example

```python
from mira_flow.core import WorkflowManager

manager = WorkflowManager.from_file("workflows/example.json")
manager.trigger({"n": 1})
manager.run_forever()   # blocks; Ctrl+C or manager.stop() to exit
```

Equivalent from the CLI (the CLI itself never publishes — `run` only
starts the workflow; publish separately, e.g. from another terminal with a
`KafkaBroker.publish()` script, same shape as `manager.trigger()` above —
see `examples/*/trigger.py` for a template):

```bash
uv run mira-flow run --workflow workflows/example.json --import examples.echo
```

## What you give up vs. `orchestrator`

| | `core` | `orchestrator` |
|---|---|---|
| Run/task history | none | `workflow_run` / `task_run` rows |
| Retry bookkeeping | in-memory counter | persisted `UP_FOR_RETRY` + `next_retry_at` |
| Crash mid-retry | attempt count lost; Kafka redelivers from attempt 1 | resumes from the ledger at `max(attempt) + 1` |
| Redelivery of an already-finished task | handler runs again | skipped via ledger dedup |
| Multiple workflows, one process | one `WorkflowManager` per config | one `Orchestrator` across all configs |
| Setup | Kafka only | Kafka + a database |

Both still commit Kafka offsets manually and are at-least-once at the broker
level — `core` just has nothing recording what happened once the handler
returns.
