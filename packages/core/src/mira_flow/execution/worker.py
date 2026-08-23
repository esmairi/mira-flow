"""Worker — the shared shape of "one job's execution unit", implemented
independently by `core.job_worker.JobWorker` (no ledger) and
`orchestrator.job_worker.JobWorker` (ledgered). Living in `execution` rather
than in `transport` or `wiring`: it isn't "how messages move" or "config
parsing" — it's an execution-loop contract, and neither `core` nor
`orchestrator` should own a contract the other has to satisfy, so both just
depend on this package instead of each other.

The two implementations still don't share a loop body — that's a deliberate
(not yet closed) gap: `orchestrator`'s handle_one threads ledger state
(workflow_run_id, task_run_id) through the retry loop and can short-circuit
before calling the handler at all (redelivery of an already-terminal task);
`core`'s can't express either without a ledger to check against. This
Protocol only captures what's genuinely identical: the outer shape a caller
(WorkflowManager, Orchestrator) can rely on.
"""

from __future__ import annotations

from typing import Protocol

from mira_flow.transport.message import Message


class Worker(Protocol):
    def handle_one(self, message: Message) -> None:
        """Process one message to completion: call the handler, route/publish
        its output (or not, if terminal), commit — however many retries it
        takes. Never raises for expected failure modes (handler exceptions,
        no matching route); an unhandled exception here is a bug."""
        ...

    def run(self) -> None:
        """Consume forever, calling handle_one for each message, until
        stop() is called. Meant to be a threading.Thread target."""
        ...

    def stop(self) -> None:
        """Signal run() to exit. May not take effect until the current
        message (or retry wait) finishes — implementations should make that
        wait interruptible where they can."""
        ...
