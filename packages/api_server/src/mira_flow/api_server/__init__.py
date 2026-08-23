"""api_server — an HTTP API over the ledger + workflow configs, for a UI
(Airflow-style): browse workflows/runs/tasks, trigger/cancel a run, retry a
failed task.

Depends on `mira-flow-core` (transport/wiring, to publish trigger/
retry messages and resolve a job's input topic) and `mira-flow-ledger`
(workflow_run/task_run history) — never `orchestrator`: this package only
ever reads/writes the ledger and publishes messages, it doesn't run job
workers.
"""

from mira_flow.api_server.app import app, create_app

__all__ = ["app", "create_app"]
