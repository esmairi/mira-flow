"""Orchestrator — orchestrates one JobWorker per (JobBinding, consumer
instance); see JobBinding.consumer_instances (§2). Mirrors
core.WorkflowManager's shape exactly: builds workers, spawns one thread per
worker, delegates run/stop to them. The actual per-message work lives in
JobWorker (job_worker.py), not here — see its docstring for how it differs
from core's."""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable
from contextlib import AbstractContextManager

from sqlalchemy.orm import Session

from mira_flow.core import JobBinding
from mira_flow.execution import Worker
from mira_flow.ledger import get_engine
from mira_flow.ledger import session_scope as default_session_scope
from mira_flow.orchestrator.job_worker import JobWorker
from mira_flow.transport import Broker

logger = logging.getLogger(__name__)

SessionScope = Callable[[], AbstractContextManager[Session]]


class Orchestrator:
    """Runs every job binding's workers, ledgered, until stopped."""

    def __init__(
        self,
        bindings: list[JobBinding],
        broker: Broker,
        session_scope: SessionScope = default_session_scope,
        *,
        poll_timeout: float = 1.0,
    ) -> None:
        """Initializes the orchestrator and builds every binding's workers.

        Args:
            bindings: The jobs to run, one JobWorker per (binding, instance).
            broker: Transport shared by every worker.
            session_scope: Context manager factory yielding a DB session.
            poll_timeout: Seconds each worker blocks on consume().
        """
        self._bindings = bindings
        self._session_scope = session_scope
        self._workers: list[Worker] = []
        self._worker_labels: list[str] = []
        for binding in bindings:
            for instance in range(binding.consumer_instances):
                self._workers.append(
                    JobWorker(binding, broker, session_scope, instance, poll_timeout)
                )
                self._worker_labels.append(
                    f"{binding.workflow_name}.{binding.task_name}#{instance}"
                )
        self._threads: list[threading.Thread] = []
        workflow_count = len({binding.workflow_name for binding in bindings})
        logger.info(
            "orchestrator: built %d worker(s) across %d job(s), %d workflow(s)",
            len(self._workers),
            len(bindings),
            workflow_count,
        )

    def entry_topic(self, workflow_name: str) -> str | None:
        """Looks up a workflow's entry job's input topic.

        Args:
            workflow_name: The workflow to look up.

        Returns:
            The entry binding's input topic, or None if the workflow has no
            entry job.
        """
        for binding in self._bindings:
            if binding.workflow_name == workflow_name and binding.is_entry:
                return binding.input_topic
        return None

    def run_forever(self) -> None:
        """Starts one thread per worker and blocks until they all exit.

        Ctrl+C (KeyboardInterrupt) triggers a clean `stop()`.
        """
        if self._session_scope is default_session_scope:
            # Force the shared ledger Engine/sessionmaker to build now, on
            # this one thread, before any JobWorker thread exists —
            # get_engine()'s lazy init has no locking (see ledger/db.py),
            # so without this, several JobWorkers could race to build it
            # the moment threads start below. Skipped when session_scope
            # was overridden (e.g. a test double): there's no shared ledger
            # engine to race on in that case.
            get_engine()
        logger.info("orchestrator: starting %d worker(s)", len(self._workers))
        self._threads = [
            threading.Thread(
                target=worker.run,
                name=f"job:{label}",
                daemon=True,
            )
            for worker, label in zip(self._workers, self._worker_labels, strict=True)
        ]
        for thread in self._threads:
            thread.start()
        try:
            while any(t.is_alive() for t in self._threads):
                for t in self._threads:
                    t.join(timeout=0.5)
        except KeyboardInterrupt:
            self.stop()
        logger.info("orchestrator: all workers stopped")

    def stop(self) -> None:
        """Signals every worker to stop and waits for their threads to exit."""
        logger.info("orchestrator: stopping")
        for worker in self._workers:
            worker.stop()
        for thread in self._threads:
            thread.join(timeout=5)
