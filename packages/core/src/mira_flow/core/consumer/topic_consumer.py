"""TopicConsumer: a Kafka consumer bound to exactly one topic, no ledger.

Independent of core/producer/ (and of transport.KafkaBroker) on purpose:
`core` favors one small, self-contained object per job over one shared
multiplexed broker object — see core/README.md.
"""

from __future__ import annotations

import logging
from typing import Any

import fastavro
from confluent_kafka import OFFSET_INVALID, Consumer, KafkaException, TopicPartition
from fastavro.validation import ValidationError as AvroValidationError

from mira_flow.core.errors import CoreBrokerError
from mira_flow.settings import settings
from mira_flow.transport.message import Message

logger = logging.getLogger(__name__)


def _bootstrap(bootstrap_servers: str | None) -> str:
    return bootstrap_servers or settings.kafka_bootstrap_servers


def _validate_payload(schema: dict[str, Any], payload: dict[str, Any], topic: str) -> None:
    """Validates a payload's shape against a topic's parsed Avro schema.

    Args:
        schema: The topic's parsed Avro schema.
        payload: The payload to validate.
        topic: The topic, for the error message.

    Raises:
        CoreBrokerError: If `payload` doesn't conform to `schema`.
    """
    try:
        fastavro.validate(payload, schema, raise_errors=True)
    except AvroValidationError as exc:
        raise CoreBrokerError(
            f"payload for topic {topic!r} does not match its avro schema: {exc}"
        ) from exc


def _check_commit(committed: list[TopicPartition] | None, topic: str) -> None:
    """Logs a warning if a synchronous offset commit reports a per-partition error.

    Unlike `Producer.flush()`'s return value (a queue-wide count that can't
    be attributed to any one message — see `topic_producer.py`), a
    `commit(message=..., asynchronous=False)` call's return is scoped to
    exactly the offset just committed, so there's no concurrent-caller
    attribution problem here: it's safe, and worth actually checking rather
    than silently discarding.

    Args:
        committed: The `TopicPartition` list `commit()` returned.
        topic: The topic, for the log message.
    """
    for tp in committed or []:
        if tp.error is not None:
            logger.warning("offset commit failed on %s: %s", topic, tp.error)


def _consumer_config(bootstrap: str, group: str, params: dict[str, Any] | None) -> dict[str, Any]:
    # Layered, lowest to highest priority: cluster-wide auth from Settings
    # (env vars / mounted secret files — never the workflow JSON config,
    # see settings.py), then this topic's params, then the structural keys
    # below, which always win — manual commit is load-bearing for our
    # at-least-once/redelivery design and can't be silently overridden.
    config = {**settings.kafka_auth_params(), **(params or {})}
    config.update(
        {
            "bootstrap.servers": bootstrap,
            "group.id": group,
            "enable.auto.commit": False,
        }
    )
    config.setdefault("auto.offset.reset", "earliest")
    return config


class TopicConsumer:
    """A consumer bound to exactly one topic, with manual offset commit."""

    def __init__(
        self,
        topic: str,
        group: str,
        bootstrap_servers: str | None = None,
        params: dict[str, Any] | None = None,
        avro_schema: dict[str, Any] | None = None,
        name: str | None = None,
    ) -> None:
        """Initializes and subscribes the underlying Consumer.

        Args:
            topic: The topic to subscribe to.
            group: The consumer group ID.
            bootstrap_servers: Kafka bootstrap servers; defaults to
                settings.kafka_bootstrap_servers.
            params: Extra confluent_kafka Consumer config (§2); structural
                keys (bootstrap.servers, group.id, enable.auto.commit) always
                win over these.
            avro_schema: This topic's parsed Avro schema (from `wiring.
                load_avro_schema`), if configured — every consumed payload
                is validated against it, treated as a poison pill on
                mismatch. None skips validation entirely.
            name: Identifies this consumer in logs (e.g. "job_name#instance"
                — see `core.manager`'s `_build_workers`). Defaults to
                `topic` if not given.
        """
        self.topic = topic
        self.name = name or topic
        self._avro_schema = avro_schema
        bootstrap = _bootstrap(bootstrap_servers)
        self._consumer = Consumer(_consumer_config(bootstrap, group, params))
        self._consumer.subscribe([topic], on_assign=self._on_assign)
        self._pending = None

    def _on_assign(self, consumer: Consumer, partitions: list[TopicPartition]) -> None:
        """Logs the partition(s) this consumer starts listening on, and the
        offset it resumes from.

        Registering an on_assign callback opts out of confluent-kafka's
        default auto-assign behavior — `consumer.assign(partitions)` below
        is what actually completes the assignment; without it the consumer
        would never start receiving messages. `consumer.position(...)`
        immediately after assign() isn't useful here — librdkafka hasn't
        resolved a fetch position yet at this point, so it always reads
        back OFFSET_INVALID; `consumer.committed(...)` (an explicit round
        trip to the broker) is what actually reports the last committed
        offset, if any.

        Args:
            consumer: The underlying Consumer being assigned (same object
                as `self._consumer` — passed in by confluent-kafka).
            partitions: The partition(s) newly assigned to this consumer.
        """
        consumer.assign(partitions)
        try:
            committed = consumer.committed(partitions, timeout=10.0)
        except KafkaException as exc:
            logger.warning("could not fetch committed offsets for %s: %s", self.name, exc)
            committed = partitions
        for tp in committed:
            offset = (
                "none (starting from auto.offset.reset)"
                if tp.offset == OFFSET_INVALID
                else tp.offset
            )
            logger.info(
                "consumer %r listening: topic=%s partition=%d offset=%s id=%s",
                self.name,
                tp.topic,
                tp.partition,
                offset,
                consumer.memberid(),
            )

    def consume(self, timeout: float = 1.0) -> Message | None:
        """Polls for the next message, or None if the poll timed out.

        A poison-pill message (one that fails to decode, or doesn't match
        this topic's Avro schema if one is configured) is committed and
        skipped rather than returned.

        Args:
            timeout: Seconds to block waiting for a message.

        Returns:
            The next message, or None if nothing arrived within `timeout`.

        Raises:
            CoreBrokerError: If the consumer fails fatally.
        """
        raw = self._consumer.poll(timeout)
        if raw is None:
            return None
        err = raw.error()
        if err is not None:
            if err.fatal():
                raise CoreBrokerError(f"consumer on {self.topic!r} failed: {err}")
            logger.warning("kafka consumer event on %s: %s", self.topic, err)
            return None
        try:
            message = Message.from_bytes(raw.topic(), raw.value())
        except ValueError:
            # Poison pill: nothing to hand the worker, commit so it doesn't loop.
            logger.warning("skipping malformed message on %s", self.topic)
            _check_commit(self._consumer.commit(message=raw, asynchronous=False), self.topic)
            return None
        if self._avro_schema is not None:
            try:
                _validate_payload(self._avro_schema, message.payload, self.topic)
            except CoreBrokerError as exc:
                logger.warning("skipping message on %s: %s", self.topic, exc)
                _check_commit(self._consumer.commit(message=raw, asynchronous=False), self.topic)
                return None
        self._pending = raw
        logger.info(
            "consumer %r consumed: topic=%s partition=%d offset=%d id=%s",
            self.name,
            raw.topic(),
            raw.partition(),
            raw.offset(),
            message.id,
        )
        return message

    def commit(self) -> None:
        """Commits the offset of the last message returned by `consume()`."""
        if self._pending is not None:
            committed = self._consumer.commit(message=self._pending, asynchronous=False)
            _check_commit(committed, self.topic)
            logger.debug("committed offset on %s", self.topic)
            self._pending = None

    def close(self) -> None:
        """Closes the underlying Consumer."""
        self._consumer.close()
