"""ProducerFactory: builds a TopicProducer per job."""

from __future__ import annotations

from typing import Any

from mira_flow.core.producer.topic_producer import TopicProducer


class ProducerFactory:
    """Builds TopicProducers that all share one bootstrap server address."""

    def __init__(self, bootstrap_servers: str | None = None) -> None:
        """Initializes the factory.

        Args:
            bootstrap_servers: Kafka bootstrap servers for every producer
                this factory builds; defaults to
                settings.kafka_bootstrap_servers.
        """
        self._bootstrap_servers = bootstrap_servers

    def create(
        self,
        topic: str,
        params: dict[str, Any] | None = None,
        avro_schema: dict[str, Any] | None = None,
        name: str | None = None,
    ) -> TopicProducer:
        """Builds a producer bound to one topic.

        Args:
            topic: The topic to publish to.
            params: Extra confluent_kafka Producer config (§2).
            avro_schema: This topic's parsed Avro schema, if configured —
                see `TopicProducer`.
            name: Identifies this producer in logs; see `TopicProducer`.

        Returns:
            The new producer.
        """
        return TopicProducer(topic, self._bootstrap_servers, params, avro_schema, name)
