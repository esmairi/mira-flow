"""TopicProducer: a Kafka producer bound to exactly one topic, no ledger.

Independent of core/consumer/ (and of transport.KafkaBroker) on purpose:
`core` favors one small, self-contained object per job over one shared
multiplexed broker object — see core/README.md.
"""

from __future__ import annotations

import logging
import time
from typing import Any

import fastavro
from confluent_kafka import Producer
from fastavro.validation import ValidationError as AvroValidationError

from mira_flow.core.errors import CoreBrokerError
from mira_flow.settings import settings
from mira_flow.transport.message import Message

logger = logging.getLogger(__name__)


def _bootstrap(bootstrap_servers: str | None) -> str:
    return bootstrap_servers or settings.kafka_bootstrap_servers


def _producer_config(bootstrap: str, params: dict[str, Any] | None) -> dict[str, Any]:
    # Layered, lowest to highest priority: cluster-wide auth from Settings
    # (env vars / mounted secret files — never the workflow JSON config,
    # see settings.py), then this topic's params, then bootstrap.servers.
    config = {**settings.kafka_auth_params(), **(params or {})}
    config.update({"bootstrap.servers": bootstrap})
    config.setdefault("acks", "all")
    config.setdefault("enable.idempotence", True)
    return config


def _wait_for_delivery(producer: Producer, outcome: list[Any], timeout: float) -> None:
    """Polls until this message's `on_delivery` callback fires, or timeout.

    Deliberately doesn't just call `Producer.flush(timeout)` — that blocks
    until the *entire* shared queue drains (`len() == 0`), not until this
    one message's callback fires. `TopicProducer` can be shared across a
    job's concurrent consumer instances (see `core.manager`'s
    `_build_producers`), so `flush()` alone could block this call for the
    full timeout on a slow *unrelated* in-flight message from another
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


class TopicProducer:
    """A producer bound to exactly one topic."""

    def __init__(
        self,
        topic: str,
        bootstrap_servers: str | None = None,
        params: dict[str, Any] | None = None,
        avro_schema: dict[str, Any] | None = None,
        name: str | None = None,
    ) -> None:
        """Initializes the underlying Producer.

        Args:
            topic: The topic this producer publishes to.
            bootstrap_servers: Kafka bootstrap servers; defaults to
                settings.kafka_bootstrap_servers.
            params: Extra confluent_kafka Producer config (§2).
            avro_schema: This topic's parsed Avro schema (from `wiring.
                load_avro_schema`), if configured — every published payload
                is validated against it before sending. None skips
                validation entirely.
            name: Identifies this producer in logs (e.g. the job name that
                owns it — see `core.manager`'s `_build_producers`). Defaults
                to `topic` if not given.
        """
        self.topic = topic
        self.name = name or topic
        self._avro_schema = avro_schema
        bootstrap = _bootstrap(bootstrap_servers)
        self._producer = Producer(_producer_config(bootstrap, params))

    def publish(self, message: Message) -> None:
        """Publishes a message and blocks until delivery is confirmed.

        Args:
            message: The message to publish; `message.kafka_key` sets the
                Kafka message key.

        Raises:
            CoreBrokerError: If this topic has a schema configured and
                `message.payload` doesn't conform to it, or if delivery
                isn't confirmed within `settings.kafka_publish_timeout_seconds`,
                or the broker reports a delivery failure.
        """
        if self._avro_schema is not None:
            _validate_payload(self._avro_schema, message.payload, self.topic)
        outcome: list[tuple[object, Any]] = []
        self._producer.produce(
            self.topic,
            value=message.to_bytes(),
            key=message.kafka_key,
            headers=message.to_headers(),
            on_delivery=lambda err, msg: outcome.append((err, msg)),
        )
        timeout = settings.kafka_publish_timeout_seconds
        _wait_for_delivery(self._producer, outcome, timeout)
        if not outcome:
            raise CoreBrokerError(f"publish to {self.topic!r} not confirmed within {timeout}s")
        err, msg = outcome[0]
        if err is not None:
            raise CoreBrokerError(f"publish to {self.topic!r} failed: {err}")
        logger.info(
            "producer %r published: topic=%s partition=%d offset=%d id=%s",
            self.name,
            msg.topic(),
            msg.partition(),
            msg.offset(),
            message.id,
        )

    def close(self) -> None:
        """Flushes any outstanding messages before shutdown.

        Unlike `publish()` (which polls for one specific message's own
        outcome — see `_wait_for_delivery`), `close()` genuinely wants the
        *whole* shared queue drained, not one message's fate — `flush()`'s
        queue-wide return value is exactly the right question to ask here,
        so (unlike in `publish()`) it's kept, and actually checked: a
        nonzero return means some messages were still undelivered when the
        timeout elapsed and are about to be silently dropped.
        """
        remaining = self._producer.flush(settings.kafka_close_timeout_seconds)
        if remaining > 0:
            logger.warning(
                "close: %d message(s) on %s still undelivered after %.1fs, dropping them",
                remaining,
                self.topic,
                settings.kafka_close_timeout_seconds,
            )
