"""Message envelope carried on every topic.

Envelope metadata (id, workflow_run_id, task_run_id, correlation_id,
force_retry) rides on Kafka message headers, not in the value — the value
is exactly the job's payload, nothing wrapped around it. That means an
externally-published bare JSON object needs no special-casing on decode,
and a payload can be Avro-schema-validated directly against the literal
wire value. Header keys reserved for this envelope are prefixed `mira-`;
everything else in `headers` is free for handlers to read and set (see
`wiring.registry.HandlerResult`).
"""

from __future__ import annotations

import json
import uuid
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

_HEADER_PREFIX = "mira-"
_ID = f"{_HEADER_PREFIX}id"
_WORKFLOW_RUN_ID = f"{_HEADER_PREFIX}workflow-run-id"
_TASK_RUN_ID = f"{_HEADER_PREFIX}task-run-id"
_CORRELATION_ID = f"{_HEADER_PREFIX}correlation-id"
_FORCE_RETRY = f"{_HEADER_PREFIX}force-retry"

# Mirrors confluent_kafka's HeadersType: what `Producer.produce(headers=...)`
# accepts and `confluent_kafka.Message.headers()` returns.
KafkaHeaders = dict[str, str | bytes | None] | list[tuple[str, str | bytes | None]] | None


def _new_id() -> str:
    return str(uuid.uuid4())


class Message(BaseModel):
    """A message carried on a topic: its payload plus envelope metadata
    that rides on Kafka headers.

    Attributes:
        topic: The topic this message was consumed from or is destined for.
        payload: The job's input or output, as JSON-serializable data — the
            literal JSON wire value, nothing wrapped around it.
        id: A unique ID for this specific message.
        workflow_run_id: The run this message belongs to, if any.
        task_run_id: The task_run (ledger row) whose attempt produced this
            message, if any — provenance, not membership: it identifies the
            attempt that published this message, not one it "belongs to"
            (the consuming job's own task_run doesn't exist yet at publish
            time). Only ever set by `orchestrator`; `core` has no ledger,
            so this is always None there.
        correlation_id: The run's correlation ID, propagated unchanged
            across every message in the chain.
        force_retry: Set when an operator (the admin API) explicitly
            re-publishes a message for a task whose last attempt is
            terminal (FAILED), to bypass `WorkflowRepository.start_task_run`'s
            redelivery dedup — which otherwise treats "last attempt is
            terminal" as "Kafka redelivered an already-processed message"
            and skips it. False on every normal message; never set by
            anything other than an explicit retry.
        headers: Free-form string headers, readable by handlers on a
            consumed message and settable on a handler's output (see
            `wiring.registry.HandlerResult`) — distinct from the `mira-`
            prefixed envelope headers above, which are reserved.
        key: The Kafka message key, as a string. None (the default) means
            "derive from workflow_run_id" — see `kafka_key` — which is what
            keeps a run's messages on one partition, and so in order.
            Settable by a handler (`HandlerResult.key`) to co-partition on
            something else instead; doing so takes over that ordering
            responsibility from workflow_run_id.
    """

    model_config = ConfigDict(frozen=True)

    topic: str
    payload: dict[str, Any]
    id: str = Field(default_factory=_new_id)
    workflow_run_id: str | None = None
    task_run_id: str | None = None
    correlation_id: str | None = None
    force_retry: bool = False
    headers: dict[str, str] = Field(default_factory=dict)
    key: str | None = None

    @field_validator("headers")
    @classmethod
    def _no_reserved_headers(cls, headers: dict[str, str]) -> dict[str, str]:
        reserved = [key for key in headers if key.startswith(_HEADER_PREFIX)]
        if reserved:
            raise ValueError(f"headers {reserved} use the reserved {_HEADER_PREFIX!r} prefix")
        return headers

    @property
    def kafka_key(self) -> bytes | None:
        """The actual Kafka message key: `key` if explicitly set, otherwise
        `workflow_run_id` — the ordering-preserving default."""
        effective = self.key if self.key is not None else self.workflow_run_id
        return effective.encode() if effective is not None else None

    def to_bytes(self) -> bytes:
        """Serializes this message's payload to JSON, for publishing as the
        Kafka value. Envelope metadata travels on headers instead — see
        `to_headers()`.

        Returns:
            The UTF-8 encoded JSON payload.
        """
        return json.dumps(self.payload).encode()

    def to_headers(self) -> list[tuple[str, str | bytes | None]]:
        """Builds this message's Kafka headers: envelope metadata plus any
        custom `headers`, for `Producer.produce(headers=...)`.

        Returns:
            The header list, UTF-8 encoded.
        """
        headers: list[tuple[str, str | bytes | None]] = [
            (_ID, self.id.encode()),
            (_FORCE_RETRY, b"1" if self.force_retry else b"0"),
        ]
        if self.workflow_run_id is not None:
            headers.append((_WORKFLOW_RUN_ID, self.workflow_run_id.encode()))
        if self.task_run_id is not None:
            headers.append((_TASK_RUN_ID, self.task_run_id.encode()))
        if self.correlation_id is not None:
            headers.append((_CORRELATION_ID, self.correlation_id.encode()))
        headers.extend((key, value.encode()) for key, value in self.headers.items())
        return headers

    @classmethod
    def from_kafka(
        cls,
        topic: str,
        data: bytes,
        headers: KafkaHeaders,
        key: bytes | None,
    ) -> Message:
        """Parses a consumed record into a Message.

        Args:
            topic: The topic this record was consumed from.
            data: The raw Kafka value — the payload, as JSON.
            headers: The raw Kafka headers, as returned by
                `confluent_kafka.Message.headers()`.
            key: The raw Kafka key, as returned by
                `confluent_kafka.Message.key()`.

        Returns:
            The parsed message.

        Raises:
            ValueError: If `data` isn't a JSON object.
        """
        payload = json.loads(data)
        if not isinstance(payload, dict):
            raise ValueError(f"message on {topic!r} is not a JSON object")
        envelope, custom = _split_headers(headers)
        return cls(
            topic=topic,
            payload=payload,
            id=envelope.get(_ID) or _new_id(),
            workflow_run_id=envelope.get(_WORKFLOW_RUN_ID),
            task_run_id=envelope.get(_TASK_RUN_ID),
            correlation_id=envelope.get(_CORRELATION_ID),
            force_retry=envelope.get(_FORCE_RETRY) == "1",
            headers=custom,
            key=key.decode(errors="replace") if key is not None else None,
        )


def _split_headers(headers: KafkaHeaders) -> tuple[dict[str, str], dict[str, str]]:
    """Splits raw Kafka headers into (envelope, custom) string dicts.

    Envelope keys are `mira-`-prefixed and reserved for this module; every
    other key is a handler-visible custom header (`Message.headers`).

    Args:
        headers: The raw Kafka headers, as returned by
            `confluent_kafka.Message.headers()`.

    Returns:
        (envelope, custom) — both decoded UTF-8, undecodable bytes replaced.
    """
    envelope: dict[str, str] = {}
    custom: dict[str, str] = {}
    items = headers.items() if isinstance(headers, dict) else headers or []
    for key, value in items:
        decoded = value.decode(errors="replace") if isinstance(value, bytes) else (value or "")
        if key.startswith(_HEADER_PREFIX):
            envelope[key] = decoded
        else:
            custom[key] = decoded
    return envelope, custom
