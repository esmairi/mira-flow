"""Message envelope carried on every topic."""

from __future__ import annotations

import json
import uuid
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


def _new_id() -> str:
    return str(uuid.uuid4())


class Message(BaseModel):
    """A message carried on a topic: a JSON envelope around a job's payload.

    Attributes:
        topic: The topic this message was consumed from or is destined for.
        payload: The job's input or output, as JSON-serializable data.
        id: A unique ID for this specific message.
        workflow_run_id: The run this message belongs to, if any; also used
            as the Kafka message key so a run's messages keep partition
            (and so ordering) affinity.
        correlation_id: The run's correlation ID, propagated unchanged
            across every message in the chain.
        force_retry: Set when an operator (the admin API) explicitly
            re-publishes a message for a task whose last attempt is
            terminal (FAILED), to bypass `WorkflowRepository.start_task_run`'s
            redelivery dedup — which otherwise treats "last attempt is
            terminal" as "Kafka redelivered an already-processed message"
            and skips it. False on every normal message; never set by
            anything other than an explicit retry.
    """

    model_config = ConfigDict(frozen=True)

    topic: str
    payload: dict[str, Any]
    id: str = Field(default_factory=_new_id)
    workflow_run_id: str | None = None
    correlation_id: str | None = None
    force_retry: bool = False

    @property
    def key(self) -> bytes | None:
        """Kafka message key: per-run ordering within a topic."""
        return self.workflow_run_id.encode() if self.workflow_run_id else None

    def to_bytes(self) -> bytes:
        """Serializes this message to its JSON envelope, for publishing.

        Returns:
            The UTF-8 encoded JSON envelope.
        """
        envelope = {
            "id": self.id,
            "payload": self.payload,
            "workflow_run_id": self.workflow_run_id,
            "correlation_id": self.correlation_id,
            "force_retry": self.force_retry,
        }
        return json.dumps(envelope).encode()

    @classmethod
    def from_bytes(cls, topic: str, data: bytes) -> Message:
        """Parses a consumed value into a Message.

        External services may publish a bare JSON object instead of the
        full envelope; in that case the whole object is the payload.

        Args:
            topic: The topic this value was consumed from.
            data: The raw consumed bytes.

        Returns:
            The parsed message.

        Raises:
            ValueError: If `data` isn't a JSON object.
        """
        obj = json.loads(data)
        if not isinstance(obj, dict):
            raise ValueError(f"message on {topic!r} is not a JSON object")
        if isinstance(obj.get("payload"), dict):
            return cls(
                topic=topic,
                payload=obj["payload"],
                id=obj.get("id") or _new_id(),
                workflow_run_id=obj.get("workflow_run_id"),
                correlation_id=obj.get("correlation_id"),
                force_retry=bool(obj.get("force_retry", False)),
            )
        return cls(topic=topic, payload=obj)
