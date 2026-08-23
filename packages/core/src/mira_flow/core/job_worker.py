"""JobWorker — one job, no ledger: consume -> handler -> produce.

Implements execution.Worker. Retries are counted in memory only (no
task_run rows, no UP_FOR_RETRY, no redelivery dedup): a crash mid-retry or
mid-handler simply loses the attempt count, and Kafka's normal redelivery
re-runs the handler from attempt 1. This package trades the DB ledger for
zero setup — use `orchestrator` when you need a run history, exactly-the-plan
retry semantics, or crash-safe resume.
"""

from __future__ import annotations

import logging
import threading
from typing import Any

from mira_flow.core.consumer import TopicConsumer
from mira_flow.core.producer import TopicProducer
from mira_flow.execution import NoMatchingRouteError, ResolvedRoute, select_route
from mira_flow.transport.message import Message
from mira_flow.wiring.registry import Handler

logger = logging.getLogger(__name__)


class JobWorker:
    """Runs one job's messages through the ledger-free consume/handle/produce loop."""

    def __init__(
        self,
        task_name: str,
        handler: Handler,
        params: dict[str, Any],
        consumer: TopicConsumer,
        producers: dict[str, TopicProducer],
        output_routes: tuple[ResolvedRoute, ...],
        retries: int = 0,
        retry_delay_seconds: float = 0.0,
        poll_timeout: float = 1.0,
    ) -> None:
        """Initializes the worker.

        Args:
            task_name: This job's name, used in logging.
            handler: The job's handler callable.
            params: Static params passed to the handler on every call.
            consumer: Consumes and commits this job's input topic.
            producers: This job's topic -> TopicProducer map.
            output_routes: This job's resolved producer routes.
            retries: Max retry attempts on handler failure.
            retry_delay_seconds: Delay between retry attempts.
            poll_timeout: Seconds to block on each `consume()` call.
        """
        self.task_name = task_name
        self._handler = handler
        self._params = params
        self._consumer = consumer
        self._producers = producers
        self._output_routes = output_routes
        self._retries = retries
        self._retry_delay_seconds = retry_delay_seconds
        self._poll_timeout = poll_timeout
        self._stop_event = threading.Event()

    def handle_one(self, message: Message) -> None:
        """Processes one message to completion, retrying on handler failure.

        Args:
            message: The consumed message to process.
        """
        logger.debug("task %s: handling message %s", self.task_name, message.id)
        for attempt in range(1, self._retries + 2):
            logger.debug("task %s: attempt %d", self.task_name, attempt)
            try:
                output = self._handler(message, self._params)
            except Exception as exc:  # noqa: BLE001 - handler failure, retried below
                if attempt <= self._retries:
                    logger.warning(
                        "task %s attempt %d failed, retrying: %s", self.task_name, attempt, exc
                    )
                    if self._wait_before_retry():
                        return
                    continue
                logger.error(
                    "task %s dropped message after %d attempt(s): %s",
                    self.task_name,
                    attempt,
                    exc,
                )
                return
            else:
                self._publish_output(output, message)
                return

    def _wait_before_retry(self) -> bool:
        """Blocks for the job's retry delay, interruptibly.

        Uses `Event.wait()` rather than `sleep()` so `stop()` cuts the wait
        short instead of blocking for the full delay.

        Returns:
            True if `stop()` was called during the wait, False if the delay
            elapsed normally (or there was no delay to wait out).
        """
        if self._retry_delay_seconds <= 0:
            return False
        logger.debug(
            "task %s: waiting %.2fs before retry", self.task_name, self._retry_delay_seconds
        )
        return self._stop_event.wait(self._retry_delay_seconds)

    def _publish_output(self, output: Any, message: Message) -> None:
        """Routes and publishes a handler's output, if it produced one.

        Args:
            output: The handler's return value; None or an empty
                `output_routes` means this job is terminal.
            message: The message being processed, for its correlation_id.

        Raises:
            NoMatchingRouteError: If `output_routes` is non-empty but no
                route matched `output`.
        """
        if output is None or not self._output_routes:
            logger.debug("task %s: succeeded, terminal", self.task_name)
            return
        topic = select_route(self._output_routes, output)
        if topic is None:
            raise NoMatchingRouteError(
                f"task {self.task_name!r}: no producer route matched output {output!r}"
            )
        logger.debug("task %s: routing output to %s", self.task_name, topic)
        self._producers[topic].publish(
            Message(topic=topic, payload=output, correlation_id=message.correlation_id)
        )

    def run(self) -> None:
        """Consumes and handles messages until `stop()` is called."""
        while not self._stop_event.is_set():
            message = self._consumer.consume(self._poll_timeout)
            if message is None:
                continue
            logger.debug(
                "task %s: received message %s on %s",
                self.task_name,
                message.id,
                self._consumer.topic,
            )
            try:
                self.handle_one(message)
            except Exception:  # noqa: BLE001 - keep the worker alive
                logger.exception("unhandled error in worker %s", self.task_name)
            finally:
                self._consumer.commit()

    def stop(self) -> None:
        """Signals `run()` to exit after the current message finishes."""
        self._stop_event.set()
