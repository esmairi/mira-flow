# cli — the `mira-flow` command

The Typer app (`cli.py`) that ties `core`, `ledger`, and `orchestrator`
together into one console script. This is the only place in the workspace
allowed to depend on all three — `core`/`ledger` never depend on each other
(see each package's own README), so composing them into runnable commands
happens here, at the root.

## Commands

| Command | Ledger | What it does |
|---|---|---|
| `init-db` | yes | Creates `workflow_run`/`task_run` (quick dev bootstrap — see `packages/ledger/README.md` for the Alembic-migrations path a real deployment should use instead) |
| `run --workflows-dir <dir>` | yes | Scans a directory of `*.json` configs, starts the ledgered `orchestrator.Orchestrator` — one process serving every workflow found |
| `run --workflow <file>` | no | Runs a single workflow config via `core.WorkflowManager` — no DB at all, just consume → handle → produce in memory |
| `trigger <workflow> <key> <json>` | yes | Creates a `workflow_run` row and publishes the first message to its `entry_consumer` topic |

`run`'s two modes are mutually exclusive — `--workflow` takes precedence and
`--workflows-dir` is ignored if both are given. Pick `--workflow` for a
quick local check of one workflow (no `init-db` needed, no DB config to get
right); pick `--workflows-dir` for anything you want run history, retries,
or redelivery dedup for. See `packages/core/src/mira_flow/core/README.md`'s
"What you give up vs. `orchestrator`" table for the full tradeoff.

Every command accepts `--log-level` (on the app itself, before the
subcommand: `mira-flow --log-level DEBUG run ...`).

## `run`

```bash
# Ledgered, one process for every workflow in ./workflows:
uv run mira-flow run --workflows-dir workflows --import examples.echo

# Ledger-free, one workflow, no DB required:
uv run mira-flow run --workflow workflows/example.json --import examples.echo
```

Both modes accept:

- `--import <module>` (repeatable) — import a Python module so its
  `@handler`-decorated functions register themselves (e.g. `examples.echo`)
- `--handlers-dir <dir>` (repeatable) — recursively import every `.py` file
  under a directory instead (or as well), no importable module required —
  see `wiring.load_handlers_from_dir`'s docstring for exactly what's skipped
  (`_`-prefixed files/dirs, e.g. `__pycache__`)

Neither mode publishes anything on its own — start `run`, then publish from
another terminal. In `--workflow` mode that's any direct producer (see
`examples/*/trigger.py` for a template — `KafkaBroker.publish()` straight to
the entry topic, no ledger involved) or, from Python, `WorkflowManager.
trigger(payload)` (see `packages/core/src/mira_flow/core/README.md`'s
example). In `--workflows-dir` mode, that's the `trigger` command below,
which needs a DB row first.

Both modes install a `SIGTERM` handler (translating a container/Kubernetes
stop signal into a clean `stop()`, same as `Ctrl+C`) — see `_install_
sigterm_handler`'s docstring in `cli.py` for why that lives here and not in
`Orchestrator`/`WorkflowManager` themselves.

## `trigger`

```bash
uv run mira-flow trigger example_workflow my-idempotency-key '{"some": "input"}'
```

Needs `init-db` (or the Alembic migrations) run first, and only works for a
workflow with an `entry_consumer` — one with none must be started by an
external publisher instead (see `examples/external-trigger/`).

## Env vars

Broker address, auth, and database URL all come from the same env vars/
`.env` file regardless of which command — see the root README's "Kafka
auth" section and `packages/core/src/mira_flow/settings.py` (transport) /
`packages/ledger/src/mira_flow/ledger/db.py` (database) for the full list.
Nothing DB- or broker-related belongs in a workflow's own JSON config —
secrets especially never should.
