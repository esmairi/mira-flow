# ledger — the workflow_run / task_run DB

What happened (§1): `WorkflowRun` / `TaskRun` ORM models, `WorkflowRepository`
(every read and write, lock_version-guarded — §6), and `session_scope()`/
`init_db()`/`get_engine()` for connecting.

Used by `orchestrator` (writes, via `WorkflowRepository`) and `admin`
(reads + `trigger`/`cancel`, also via `WorkflowRepository` — never raw
queries scattered across routers, see its module docstring). `core` never
depends on this package — that's the whole point of `core`.

## Schema migrations

Two ways to get the tables into a database — **use only one against any
given database, never both**, since `init_db()`'s `create_all()` doesn't
know about Alembic's migration-history table and vice versa:

- **`make init-db`** (`mira-flow init-db`) — quick local/dev
  bootstrap. Creates whatever tables are missing from the current model
  definitions. No history, so it can't evolve an existing schema — only
  useful against a fresh/empty database.
- **`make migrate`** (`alembic -c packages/ledger/alembic.ini upgrade
  head`) — the real path. Tracked, ordered, forward *and* backward
  (`downgrade`) schema changes. Use this anywhere the schema needs to
  change over time, i.e. any real deployment.

Both read the same `DATABASE_URL`/`.env` (`mira_flow.ledger.db.
settings`) — there's no separate URL to configure for Alembic.

### Changing the schema

1. Edit `src/mira_flow/ledger/models.py`.
2. `make migration name="describe the change"` — autogenerates a revision
   in `alembic/versions/` by diffing the models against whatever
   `DATABASE_URL` currently points at (point it at a copy of production's
   current schema, or a freshly-`make migrate`'d dev DB, so the diff is
   accurate).
3. **Read the generated file.** Autogenerate catches column/index/
   constraint changes; it does not reliably catch data migrations, column
   renames (it'll see those as drop+add and lose the data), or some
   SQLite-specific constraints — verify it did what you meant, by hand.
4. `make migrate` to apply it, commit the migration file alongside the
   model change.

`alembic/versions/` is excluded from `ruff` — those files are generated
and read once, not hand-styled.
