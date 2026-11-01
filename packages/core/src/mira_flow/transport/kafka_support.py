"""confluent-kafka plumbing shared by `transport.KafkaBroker` and `core`'s
TopicProducer/TopicConsumer: thread-safe delivery confirmation, and making
sure a topic exists before a consumer subscribes to it.
"""

from __future__ import annotations

import logging
import threading
from typing import Any

from confluent_kafka import KafkaError, KafkaException, Producer
from confluent_kafka import Message as RawMessage
from confluent_kafka.admin import AdminClient, NewTopic

from mira_flow.settings import settings

logger = logging.getLogger(__name__)

DeliveryOutcome = tuple[KafkaError | None, RawMessage]


class DeliveryPoller:
    """Serves one Producer's delivery callbacks from a single background thread.

    Delivery callbacks only fire inside `Producer.poll()`/`flush()`, on
    whichever thread happens to call it. When several threads share a
    Producer and each polls for its *own* message, thread A's `poll()` can
    fire thread B's callback — and B then sits in its own `poll()` until
    the full publish timeout, although its message was acked long ago
    (with 8 job threads sharing one Producer, that turned nearly every
    publish into a 30s wait). Here exactly one thread polls; each publisher
    waits on its own Event, set by its own callback, wherever it fires.

    Args:
        producer: The Producer to serve. Callers must not `poll()` it
            themselves — only `produce()` via `produce_and_wait()`.
        name: Suffix for the polling thread's name, for debugging.
    """

    def __init__(self, producer: Producer, name: str) -> None:
        self.producer = producer
        self._stop = threading.Event()
        self._thread = threading.Thread(
            target=self._run, name=f"kafka-delivery-{name}", daemon=True
        )
        self._thread.start()

    def _run(self) -> None:
        while not self._stop.is_set():
            self.producer.poll(0.1)

    def produce_and_wait(
        self,
        topic: str,
        *,
        value: bytes,
        key: str | bytes | None,
        headers: Any,
        timeout: float,
    ) -> DeliveryOutcome | None:
        """Produces one message and blocks until its own delivery report.

        Returns:
            The (error, message) pair from the delivery callback, or None if
            no report arrived within `timeout`.
        """
        done = threading.Event()
        outcome: list[DeliveryOutcome] = []

        def on_delivery(err: KafkaError | None, msg: RawMessage) -> None:
            outcome.append((err, msg))
            done.set()

        self.producer.produce(topic, value=value, key=key, headers=headers, on_delivery=on_delivery)
        done.wait(timeout)
        return outcome[0] if outcome else None

    def close(self, timeout: float) -> int:
        """Stops the polling thread, then drains the whole queue.

        Returns:
            How many messages were still undelivered after `timeout`.
        """
        self._stop.set()
        self._thread.join(timeout=1.0)
        return self.producer.flush(timeout)


_known_topics: dict[str, set[str]] = {}
_known_lock = threading.Lock()


def ensure_topic(bootstrap: str, topic: str) -> None:
    """Creates `topic` if it doesn't exist yet (broker-default partitions and
    replication), so a consumer can subscribe to a real topic.

    Why: with broker-side auto-creation, a topic only appears on its first
    *publish*. A consumer that subscribed before that doesn't see the new
    topic until its next metadata refresh (librdkafka's
    `topic.metadata.refresh.interval.ms`, 5 min by default) — and every
    consumer of a workflow shares one group, so each late-appearing topic
    also triggers a rebalance of all of them. Downstream jobs sat idle with
    a growing backlog.

    Best effort: disabled by `settings.kafka_create_missing_topics=False`
    (for clusters where topics are provisioned out of band); a failure
    (e.g. no CreateTopics ACL) is logged and the consumer subscribes
    anyway, exactly as before.

    Args:
        bootstrap: Kafka bootstrap servers.
        topic: The topic to ensure.
    """
    if not settings.kafka_create_missing_topics:
        return
    with _known_lock:
        if topic in _known_topics.get(bootstrap, set()):
            return
        admin = AdminClient({**settings.kafka_auth_params(), "bootstrap.servers": bootstrap})
        try:
            existing = set(admin.list_topics(timeout=10).topics)
            _known_topics[bootstrap] = existing
            if topic in existing:
                return
            future = admin.create_topics(
                [NewTopic(topic, num_partitions=-1, replication_factor=-1)]
            )[topic]
            future.result(timeout=15)
            logger.info("created missing topic %r", topic)
        except KafkaException as exc:
            error = exc.args[0] if exc.args else None
            if not (
                isinstance(error, KafkaError) and error.code() == KafkaError.TOPIC_ALREADY_EXISTS
            ):
                logger.warning("could not create topic %r (subscribing anyway): %s", topic, exc)
                return
        except Exception as exc:  # noqa: BLE001 - best effort, never block startup
            logger.warning("could not create topic %r (subscribing anyway): %s", topic, exc)
            return
        _known_topics.setdefault(bootstrap, set()).add(topic)
