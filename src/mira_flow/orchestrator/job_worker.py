"""JobWorker — one (JobBinding, consumer instance)'s execution unit: consume
-> handler -> produce, with the ledger written at every step (§5).
Implements execution.Worker — see its docstring for what's genuinely shared
with core.job_worker.JobWorker vs. what isn't: this one threads
workflow_run_id/task_run_id through the retry loop and can short-circuit
before calling the handler at all on redelivery of an already-terminal task;
core's can't express either without a ledger.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable
from contextlib import AbstractContextManager
from datetime import UTC, datetime, timedelta

from sqlalchemy.orm import Session

from mira_flow.core import JobBinding
from mira_flow.execution import NoMatchingRouteError, select_route
from mira_flow.ledger import WorkflowRepository
from mira_flow.transport import Broker, Message
from mira_flow.wiring.registry import HandlerOutput, split_handler_output

logger = logging.getLogger(__name__)

SessionScope = Callable[[], AbstractContextManager[Session]]

AttemptState = tuple[str, str | None, str | None]
"""(task_run_id, workflow_run_id, correlation_id) for one attempt."""


class JobWorker:
    """Runs one job's messages through the DB-ledgered consume/handle/produce
    loop.

    Attributes:
        task_name: The job name this worker processes, from `binding`.
    """

    def __init__(
        self,
        binding: JobBinding,
        broker: Broker,
        session_scope: SessionScope,
        instance: int = 0,
        poll_timeout: float = 1.0,
    ) -> None:
        """Initializes the worker.

        Args:
            binding: The job's resolved config (topics, handler, retry policy).
            broker: Transport used to consume, commit, and publish messages.
            session_scope: Context manager factory yielding a DB session.
            instance: This worker's index among its job's parallel consumers.
            poll_timeout: Seconds to block on each `consume()` call.
        """
        self.task_name = binding.task_name
        self._binding = binding
        self._broker = broker
        self._session_scope = session_scope
        self._instance = instance
        self._poll_timeout = poll_timeout
        self._stop_event = threading.Event()

    def handle_one(self, message: Message) -> None:
        """Processes one message to completion, retrying on handler failure.

        Args:
            message: The consumed message to process.
        """
        binding = self._binding
        logger.debug(
            "task %s: handling message %s (workflow_run_id=%s)",
            binding.task_name,
            message.id,
            message.workflow_run_id,
        )
        # Carried across attempts: on retry we must reuse the same
        # workflow_run created by the first attempt, not re-derive it from
        # the original message (which stays workflow_run_id=None for an
        # externally-triggered entry message) — otherwise every retry mints
        # a fresh implicit run and orphans the previous one.
        workflow_run_id = message.workflow_run_id
        correlation_id = message.correlation_id

        for attempt in range(1, binding.retries + 2):
            attempt_state = self._start_attempt(message, workflow_run_id, correlation_id)
            if attempt_state is None:
                logger.debug(
                    "task %s: message %s is a redelivery of an already-terminal task, skipping",
                    binding.task_name,
                    message.id,
                )
                break
            task_run_id, workflow_run_id, correlation_id = attempt_state
            logger.debug(
                "task %s: attempt %d, task_run_id=%s", binding.task_name, attempt, task_run_id
            )

            try:
                output = binding.handler(message, binding.params)
            except Exception as exc:  # noqa: BLE001 - handler failure, retried below
                if self._record_failure(task_run_id, exc, attempt):
                    if self._wait_before_retry():
                        return  # shutting down; leave the offset uncommitted
                    continue
                break
            else:
                self._record_success(task_run_id, output, workflow_run_id, correlation_id)
                break

        self._broker.commit(message, instance=self._instance)
        logger.debug("task %s: committed message %s", binding.task_name, message.id)

    def _start_attempt(
        self,
        message: Message,
        workflow_run_id: str | None,
        correlation_id: str | None,
    ) -> AttemptState | None:
        """Records the start of one attempt in the ledger.

        Args:
            message: The message being processed.
            workflow_run_id: The run ID carried from a prior attempt, if any.
            correlation_id: The correlation ID carried from a prior attempt.

        Returns:
            The attempt's (task_run_id, workflow_run_id, correlation_id), or
            None if this message is a redelivery of an already-terminal task.
        """
        binding = self._binding
        with self._session_scope() as session:
            repo = WorkflowRepository(session)
            task_run = repo.start_task_run(
                workflow_name=binding.workflow_name,
                workflow_version=binding.workflow_version,
                task_name=binding.task_name,
                is_entry=binding.is_entry,
                workflow_run_id=workflow_run_id,
                correlation_id=correlation_id,
                input=message.payload,
                force=message.force_retry,
            )
            if task_run is None:
                return None
            return task_run.id, task_run.workflow_run_id, task_run.correlation_id

    def _record_failure(self, task_run_id: str, exc: Exception, attempt: int) -> bool:
        """Records a handler failure and decides whether to retry.

        Args:
            task_run_id: The task_run row this attempt belongs to.
            exc: The exception raised by the handler.
            attempt: The 1-indexed attempt number that just failed.

        Returns:
            True if the job has retries left and should be attempted again,
            False if it was just marked permanently failed.
        """
        binding = self._binding
        should_retry = attempt <= binding.retries
        with self._session_scope() as session:
            repo = WorkflowRepository(session)
            if should_retry:
                next_retry_at = datetime.now(UTC) + timedelta(seconds=binding.retry_delay_seconds)
                repo.retry_task_run(task_run_id, exc, next_retry_at)
            else:
                repo.fail_task_run(task_run_id, exc)

        if should_retry:
            logger.warning(
                "task %s attempt %d failed, retrying: %s", binding.task_name, attempt, exc
            )
        else:
            logger.error(
                "task %s exhausted retries after attempt %d: %s",
                binding.task_name,
                attempt,
                exc,
            )
        return should_retry

    def _wait_before_retry(self) -> bool:
        """Blocks for the job's retry delay, interruptibly.

        Uses `Event.wait()` rather than `sleep()` so `stop()` cuts the wait
        short instead of blocking for the full delay: retrying into a dying
        process would just be abandoned anyway, and the offset stays
        uncommitted, so redelivery resumes it correctly from the ledger
        after restart.

        Returns:
            True if `stop()` was called during the wait, False if the delay
            elapsed normally (or there was no delay to wait out).
        """
        delay = self._binding.retry_delay_seconds
        if delay <= 0:
            return False
        logger.debug("task %s: waiting %.2fs before retry", self._binding.task_name, delay)
        return self._stop_event.wait(delay)

    def _record_success(
        self,
        task_run_id: str,
        output: HandlerOutput,
        workflow_run_id: str | None,
        correlation_id: str | None,
    ) -> None:
        """Routes the handler's output, records it, and publishes downstream.

        A routing, recording, or publish failure is not retried — it's
        recorded as a task failure, the same as an unmatched route.

        Args:
            task_run_id: The task_run row to record the outcome against;
                also stamped on the outgoing message as its provenance
                (`Message.task_run_id`).
            output: The handler's return value; None (or a HandlerResult
                whose payload is None) means terminal.
            workflow_run_id: This run's ID, for the outgoing message.
            correlation_id: This run's correlation ID, for the outgoing message.
        """
        binding = self._binding
        try:
            payload, headers, key = split_handler_output(output)
            chosen_topic = (
                select_route(binding.output_routes, payload) if payload is not None else None
            )
            if binding.output_routes and payload is not None and chosen_topic is None:
                # Handler succeeded but nothing declared matches its output —
                # not a handler failure, not retried, same bucket as a
                # record/publish failure below.
                raise NoMatchingRouteError(
                    f"task {binding.task_name!r}: no producer route matched output {payload!r}"
                )
            terminal = chosen_topic is None
            # Record first, durably, in its own committed transaction.
            with self._session_scope() as session:
                repo = WorkflowRepository(session)
                repo.succeed_task_run(task_run_id, payload, terminal=terminal)
            if terminal:
                logger.debug("task %s: succeeded, terminal", binding.task_name)
            else:
                logger.debug("task %s: routing output to %s", binding.task_name, chosen_topic)
                # chosen_topic is only ever set (non-None) when payload is
                # also non-None — see the ternary above.
                assert payload is not None
                next_message = Message(
                    topic=chosen_topic,
                    payload=payload,
                    workflow_run_id=workflow_run_id,
                    task_run_id=task_run_id,
                    correlation_id=correlation_id,
                    headers=headers,
                    key=key,
                )
                self._broker.publish(chosen_topic, next_message)
        except Exception as exc:  # noqa: BLE001 - record, routing, or publish failure — not retried
            with self._session_scope() as session:
                repo = WorkflowRepository(session)
                repo.fail_task_run(task_run_id, exc)

    def run(self) -> None:
        """Consumes and handles messages until `stop()` is called."""
        binding = self._binding
        instance = self._instance
        while not self._stop_event.is_set():
            message = self._broker.consume(
                binding.input_topic, timeout=self._poll_timeout, instance=instance
            )
            if message is None:
                continue
            logger.debug(
                "task %s#%d: received message %s on %s",
                binding.task_name,
                instance,
                message.id,
                binding.input_topic,
            )
            try:
                self.handle_one(message)
            except Exception:  # noqa: BLE001 - keep the worker alive
                logger.exception(
                    "unhandled error processing message on %s#%d", binding.input_topic, instance
                )

    def stop(self) -> None:
        """Signals `run()` to exit after the current message finishes."""
        self._stop_event.set()
