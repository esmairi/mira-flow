"""ConsumerFactory: builds a TopicConsumer per job."""

from __future__ import annotations

from typing import Any

from mira_flow.core.consumer.topic_consumer import TopicConsumer


class ConsumerFactory:
    """Builds TopicConsumers that all share one bootstrap server address."""

    def __init__(self, bootstrap_servers: str | None = None) -> None:
        """Initializes the factory.

        Args:
            bootstrap_servers: Kafka bootstrap servers for every consumer
                this factory builds; defaults to
                settings.kafka_bootstrap_servers.
        """
        self._bootstrap_servers = bootstrap_servers

    def create(
        self,
        topic: str,
        group: str,
        params: dict[str, Any] | None = None,
        avro_schema: dict[str, Any] | None = None,
        name: str | None = None,
    ) -> TopicConsumer:
        """Builds a consumer bound to one topic.

        Args:
            topic: The topic to subscribe to.
            group: The consumer group ID.
            params: Extra confluent_kafka Consumer config (§2).
            avro_schema: This topic's parsed Avro schema, if configured —
                see `TopicConsumer`.
            name: Identifies this consumer in logs; see `TopicConsumer`.

        Returns:
            The new consumer, already subscribed.
        """
        return TopicConsumer(topic, group, self._bootstrap_servers, params, avro_schema, name)
