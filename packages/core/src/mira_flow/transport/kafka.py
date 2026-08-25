"""KafkaBroker — the external-broker implementation of the Broker protocol."""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Mapping
from typing import Any

import fastavro
from confluent_kafka import (
    OFFSET_INVALID,
    Consumer,
    KafkaError,
    KafkaException,
    Producer,
    TopicPartition,
)
from confluent_kafka import Message as RawMessage
from fastavro.validation import ValidationError as AvroValidationError

from mira_flow.settings import settings
from mira_flow.transport.message import Message

logger = logging.getLogger(__name__)


class BrokerError(Exception):
    """Raised when a publish cannot be confirmed or a consumer fails fatally."""


def _make_on_assign(name: str) -> Any:
    """Builds an on_assign callback that logs the partition(s) a consumer
    starts listening on, and the offset it resumes from.

    Registering an on_assign callback opts out of confluent-kafka's default
    auto-assign behavior — `consumer.assign(partitions)` inside it is what
    actually completes the assignment; without it the consumer would never
    start receiving messages. `consumer.position(...)` immediately after
    assign() isn't useful here — librdkafka hasn't resolved a fetch
    position yet at this point, so it always reads back OFFSET_INVALID;
    `consumer.committed(...)` (an explicit round trip to the broker) is
    what actually reports the last committed offset, if any.

    Args:
        name: Identifies this consumer in logs, e.g. "group:topic#instance".

    Returns:
        A callback suitable for `Consumer.subscribe(..., on_assign=...)`.
    """

    def on_assign(consumer: Consumer, partitions: list[TopicPartition]) -> None:
        consumer.assign(partitions)
        try:
            committed = consumer.committed(partitions, timeout=10.0)
        except KafkaException as exc:
            logger.warning("could not fetch committed offsets for %r: %s", name, exc)
            committed = partitions
        for tp in committed:
            offset = (
                "none (starting from auto.offset.reset)"
                if tp.offset == OFFSET_INVALID
                else tp.offset
            )
            logger.info(
                "consumer %r listening: topic=%s partition=%d offset=%s id=%s",
                name,
                tp.topic,
                tp.partition,
                offset,
                consumer.memberid(),
            )

    return on_assign


def _check_commit(committed: list[TopicPartition] | None, topic: str, instance: int) -> None:
    """Logs a warning if a synchronous offset commit reports a per-partition error.

    Unlike `Producer.flush()`'s return value (a queue-wide count shared
    across every concurrent publisher — see `publish()` below), a
    `commit(message=..., asynchronous=False)` call's return is scoped to
    exactly the offset just committed, so there's no cross-caller
    attribution problem here: it's safe, and worth actually checking rather
    than silently discarding.

    Args:
        committed: The `TopicPartition` list `commit()` returned.
        topic: The topic, for the log message.
        instance: The consumer instance index, for the log message.
    """
    for tp in committed or []:
        if tp.error is not None:
            logger.warning("offset commit failed on %s#%d: %s", topic, instance, tp.error)


def _wait_for_delivery(
    producer: Producer, outcome: list[tuple[KafkaError | None, RawMessage]], timeout: float
) -> None:
    """Polls until this message's `on_delivery` callback fires, or timeout.

    Deliberately doesn't just call `Producer.flush(timeout)` — that blocks
    until the *entire* shared queue drains (`len() == 0`), not until this
    one message's callback fires. `KafkaBroker` shares one Producer across
    every topic and caller, so `flush()` alone could block this call for
    the full timeout on a slow *unrelated* in-flight message from another
    caller, even though this message was already acked. Polling and
    checking `outcome` directly bounds the wait to exactly this message.

    Args:
        producer: The Producer to poll.
        outcome: The list this message's `on_delivery` callback appends to;
            polling stops as soon as it's non-empty.
        timeout: Max seconds to wait.
    """
    deadline = time.monotonic() + timeout
    while not outcome:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return
        producer.poll(remaining)


def _validate_payload(schema: dict[str, Any], payload: dict[str, Any], topic: str) -> None:
    """Validates a payload's shape against a topic's parsed Avro schema.

    Message payloads stay plain JSON on the wire (§8) — this checks shape
    conformance against the schema, it doesn't do Avro binary encoding.

    Args:
        schema: The topic's parsed Avro schema.
        payload: The payload to validate.
        topic: The topic, for the error message.

    Raises:
        BrokerError: If `payload` doesn't conform to `schema`.
    """
    try:
        fastavro.validate(payload, schema, raise_errors=True)
    except AvroValidationError as exc:
        raise BrokerError(
            f"payload for topic {topic!r} does not match its avro schema: {exc}"
        ) from exc


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


def _producer_config(bootstrap: str, params: dict[str, Any] | None) -> dict[str, Any]:
    config = {**settings.kafka_auth_params(), **(params or {})}
    config.update({"bootstrap.servers": bootstrap})
    config.setdefault("acks", "all")
    config.setdefault("enable.idempotence", True)
    return config


class KafkaBroker:
    """confluent-kafka adapter.

    One producer (acks=all, idempotent by default) shared by all publishers;
    one consumer per (topic, instance), created lazily with manual offset
    commit. A topic normally has one instance (§2 ConsumerSpec.instances
    defaults to 1) — set it higher to run several Consumer clients in the
    same group, so Kafka splits the topic's partitions across them; safe for
    our per-run ledger semantics regardless, since Message.kafka_key
    defaults to workflow_run_id, routing every run's messages to one
    partition unless a handler overrides it. Pending offsets are tracked
    per (topic, instance).

    `consumer_params` / `producer_params` are extra confluent_kafka client
    config, from each topic's ConsumerSpec/ProducerSpec.params (§2). Because
    this broker multiplexes every producer topic behind one shared Producer
    instance, `producer_params` is necessarily a single dict applied to that
    one instance — callers merge params across all their producer topics
    before constructing this broker (see cli.py); per-topic producer tuning
    isn't possible here the way it is in `core`, which uses one Producer per
    topic instead.

    Cluster auth (`security.protocol`/`sasl.*`/`ssl.*`) is never expected in
    `consumer_params`/`producer_params` — it comes from `settings.
    kafka_auth_params()` instead (env vars or mounted secret files, see
    settings.py), merged in underneath these params so a topic could still
    override it if it ever genuinely needed different credentials.

    `consumer_schemas` / `producer_schemas` (topic -> parsed Avro schema,
    from `wiring.load_avro_schema` against each topic's `schema_avro_value`)
    are optional, per-topic runtime payload validation: `publish()` rejects
    a non-conforming payload before sending; `consume()` treats one like a
    poison pill (logged, skipped, offset committed) rather than handing it
    to a worker. A topic with nothing in these mappings isn't validated —
    same as today, message payloads stay plain JSON on the wire either way
    (§8).
    """

    def __init__(
        self,
        bootstrap_servers: str | None = None,
        consumer_groups: Mapping[str, str] | None = None,
        consumer_params: Mapping[str, dict[str, Any]] | None = None,
        producer_params: dict[str, Any] | None = None,
        default_group: str = "mira-flow",
        consumer_schemas: Mapping[str, dict[str, Any]] | None = None,
        producer_schemas: Mapping[str, dict[str, Any]] | None = None,
    ) -> None:
        """Initializes the broker. Producer and consumers are built lazily.

        Args:
            bootstrap_servers: Kafka bootstrap servers; defaults to
                settings.kafka_bootstrap_servers.
            consumer_groups: Per-topic consumer group ID, keyed by topic;
                topics not listed use `default_group`.
            consumer_params: Per-topic extra confluent_kafka Consumer
                config, keyed by topic.
            producer_params: Extra confluent_kafka Producer config, applied
                to the one shared Producer.
            default_group: Consumer group used for any topic not listed in
                `consumer_groups`.
            consumer_schemas: Per-topic parsed Avro schema to validate
                consumed payloads against, keyed by topic. Topics not
                listed aren't validated.
            producer_schemas: Per-topic parsed Avro schema to validate
                published payloads against, keyed by topic. Topics not
                listed aren't validated.
        """
        self._bootstrap = bootstrap_servers or settings.kafka_bootstrap_servers
        self._groups = dict(consumer_groups or {})
        self._consumer_params = dict(consumer_params or {})
        self._producer_params = dict(producer_params or {})
        self._default_group = default_group
        self._consumer_schemas = dict(consumer_schemas or {})
        self._producer_schemas = dict(producer_schemas or {})
        self._producer: Producer | None = None
        self._consumers: dict[tuple[str, int], Consumer] = {}
        self._pending: dict[tuple[str, int], RawMessage] = {}
        self._lock = threading.Lock()

    # -- publish ------------------------------------------------------------

    def publish(self, topic: str, message: Message) -> None:
        """Publishes synchronously.

        Args:
            topic: The topic to publish to.
            message: The message to publish.

        Raises:
            BrokerError: If `topic` has a schema configured and `message.
                payload` doesn't conform to it, or if delivery isn't
                confirmed within `settings.kafka_publish_timeout_seconds`,
                or the broker reports a delivery failure.
        """
        schema = self._producer_schemas.get(topic)
        if schema is not None:
            _validate_payload(schema, message.payload, topic)
        producer = self._get_producer()
        outcome: list[tuple[KafkaError | None, RawMessage]] = []
        producer.produce(
            topic,
            value=message.to_bytes(),
            key=message.kafka_key,
            headers=message.to_headers(),
            on_delivery=lambda err, msg: outcome.append((err, msg)),
        )
        timeout = settings.kafka_publish_timeout_seconds
        _wait_for_delivery(producer, outcome, timeout)
        if not outcome:
            raise BrokerError(f"publish to {topic!r} not confirmed within {timeout}s")
        err, msg = outcome[0]
        if err is not None:
            raise BrokerError(f"publish to {topic!r} failed: {err}")
        logger.info(
            "producer published: topic=%s partition=%d offset=%d id=%s",
            msg.topic(),
            msg.partition(),
            msg.offset(),
            message.id,
        )

    # -- consume / commit ---------------------------------------------------

    def consume(
        self, topic: str, timeout: float | None = None, instance: int = 0
    ) -> Message | None:
        """Blocks for the next message on the topic.

        Args:
            topic: The topic to consume from.
            timeout: Seconds to block waiting for a message; None blocks
                indefinitely.
            instance: Which of the topic's parallel consumer instances (§2
                ConsumerSpec.instances) to poll. Pass the same `instance`
                to the matching `commit()` call.

        Returns:
            The next message, or None on timeout.

        Raises:
            BrokerError: If the consumer fails fatally.
        """
        consumer = self._consumer_for(topic, instance)
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            remaining = 1.0 if deadline is None else min(1.0, deadline - time.monotonic())
            if remaining <= 0:
                return None
            raw = consumer.poll(remaining)
            if raw is None:
                continue
            message = self._decode(raw, consumer, topic, instance)
            if message is None:
                continue
            self._pending[(topic, instance)] = raw
            group = self._groups.get(topic, self._default_group)
            logger.info(
                "consumer %r consumed: topic=%s partition=%d offset=%d id=%s",
                f"{group}:{topic}#{instance}",
                raw.topic(),
                raw.partition(),
                raw.offset(),
                message.id,
            )
            return message

    def _decode(
        self, raw: RawMessage, consumer: Consumer, topic: str, instance: int
    ) -> Message | None:
        """Validates and decodes one polled record.

        Args:
            raw: The record returned by `Consumer.poll()`.
            consumer: Used to commit-and-skip a malformed record.
            topic: The topic being consumed, for error messages.
            instance: The consumer instance index, for error messages.

        Returns:
            The decoded message, or None if this record should be skipped
            — a transient Kafka event, or a poison pill (already committed
            so it can't be redelivered forever).

        Raises:
            BrokerError: If the record carries a fatal consumer error.
        """
        err = raw.error()
        if err is not None:
            if err.fatal():
                raise BrokerError(f"consumer on {topic!r} (instance {instance}) failed: {err}")
            logger.warning("kafka consumer event on %s#%d: %s", topic, instance, err)
            return None
        raw_topic, data = raw.topic(), raw.value()
        if raw_topic is None or data is None:
            # Tombstone (null-value) message, or a topic-less raw message —
            # same as a malformed one: nothing to hand the worker.
            logger.warning("skipping message with no topic/value on %s#%d", topic, instance)
            _check_commit(consumer.commit(message=raw, asynchronous=False), topic, instance)
            return None
        try:
            message = Message.from_kafka(raw_topic, data, raw.headers(), raw.key())
        except ValueError:
            logger.warning("skipping malformed message on %s#%d", topic, instance)
            _check_commit(consumer.commit(message=raw, asynchronous=False), topic, instance)
            return None
        schema = self._consumer_schemas.get(topic)
        if schema is not None:
            try:
                _validate_payload(schema, message.payload, topic)
            except BrokerError as exc:
                # Same treatment as a poison pill above: a schema mismatch
                # is a producer bug, not something a worker can act on —
                # log, commit so it isn't redelivered forever, move on.
                logger.warning("skipping message on %s#%d: %s", topic, instance, exc)
                _check_commit(consumer.commit(message=raw, asynchronous=False), topic, instance)
                return None
        return message

    def commit(self, message: Message, instance: int = 0) -> None:
        """Commits the consumer offset of a previously consumed message.

        Args:
            message: A message previously returned by `consume()`.
            instance: Must match the `instance` passed to that `consume()`
                call.
        """
        key = (message.topic, instance)
        raw = self._pending.pop(key, None)
        if raw is None:
            return
        committed = self._consumers[key].commit(message=raw, asynchronous=False)
        _check_commit(committed, message.topic, instance)
        logger.debug(
            "committed offset for message %s on %s#%d", message.id, message.topic, instance
        )

    def close(self) -> None:
        """Closes every consumer and flushes the producer, if either exists.

        Unlike `publish()` (which polls for one specific message's own
        outcome — see `_wait_for_delivery`), this genuinely wants the
        *whole* shared queue drained, not one message's fate — `flush()`'s
        queue-wide return value is exactly the right question to ask here,
        so (unlike in `publish()`) it's kept, and actually checked: a
        nonzero return means some messages were still undelivered when the
        timeout elapsed and are about to be silently dropped.
        """
        with self._lock:
            consumers = list(self._consumers.values())
            self._consumers.clear()
            self._pending.clear()
        for consumer in consumers:
            consumer.close()
        if self._producer is not None:
            remaining = self._producer.flush(settings.kafka_close_timeout_seconds)
            if remaining > 0:
                logger.warning(
                    "close: %d message(s) still undelivered after %.1fs, dropping them",
                    remaining,
                    settings.kafka_close_timeout_seconds,
                )

    # -- internals ----------------------------------------------------------

    def _get_producer(self) -> Producer:
        """Returns the shared Producer, creating it on first use."""
        with self._lock:
            if self._producer is None:
                logger.debug("creating shared producer (bootstrap=%s)", self._bootstrap)
                self._producer = Producer(_producer_config(self._bootstrap, self._producer_params))
            return self._producer

    def _consumer_for(self, topic: str, instance: int) -> Consumer:
        """Returns the Consumer for (topic, instance), creating it on first use.

        Args:
            topic: The topic to consume from.
            instance: Which parallel consumer instance this is.

        Returns:
            The (possibly newly created and subscribed) consumer.
        """
        key = (topic, instance)
        with self._lock:
            consumer = self._consumers.get(key)
            if consumer is None:
                # Same group across all instances of a topic — that's what
                # makes Kafka split the topic's partitions across them.
                group = self._groups.get(topic, self._default_group)
                consumer = Consumer(
                    _consumer_config(self._bootstrap, group, self._consumer_params.get(topic))
                )
                name = f"{group}:{topic}#{instance}"
                consumer.subscribe([topic], on_assign=_make_on_assign(name))
                self._consumers[key] = consumer
                logger.debug("created consumer for %s#%d (group=%s)", topic, instance, group)
            return consumer
