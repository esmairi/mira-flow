"""Broker protocol — the only transport surface the rest of the code sees."""

from __future__ import annotations

from typing import Protocol

from mira_flow.transport.message import Message


class Broker(Protocol):
    """Structural type for anything that can publish/consume/commit messages."""

    def publish(self, topic: str, message: Message) -> None:
        """Publishes synchronously.

        Args:
            topic: The topic to publish to.
            message: The message to publish.

        Raises:
            BrokerError: If delivery fails or can't be confirmed.
        """
        ...

    def consume(self, topic: str, timeout: float | None = None, instance: int = 0) -> Message | None:
        """Blocks for the next message on the topic.

        Args:
            topic: The topic to consume from.
            timeout: Seconds to block waiting for a message; None blocks
                indefinitely.
            instance: Which of the topic's parallel consumer instances (§2
                ConsumerSpec.instances) to poll — each is its own Consumer
                client in the same group, so Kafka splits the topic's
                partitions across them. Pass the same `instance` to the
                matching `commit()` call.

        Returns:
            The next message, or None on timeout.
        """
        ...

    def commit(self, message: Message, instance: int = 0) -> None:
        """Commits the consumer offset of a previously consumed message.

        Args:
            message: A message previously returned by `consume()`.
            instance: Must match the `instance` passed to that `consume()`
                call.
        """
        ...
