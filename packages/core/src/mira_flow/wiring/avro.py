"""Avro schema loading for runtime message payload validation.

Separate from `config.py`'s `_validate_avro_schema`, which checks a schema
*file* is well-formed at config-parse time and then discards the parsed
result. This loads an already-validated schema once, for reuse by the
transport layer against every message it publishes/consumes on that topic
— see `transport.kafka`'s `producer_schemas`/`consumer_schemas` and
`core`'s `TopicConsumer`/`TopicProducer` `avro_schema` param.

Message payloads stay plain JSON on the wire in v1 (§8) — this validates
that a payload's *shape* conforms to its topic's Avro schema, it doesn't
switch to Avro binary encoding.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import fastavro


def load_avro_schema(schema_ref: str | None, base_dir: Path) -> dict[str, Any] | None:
    """Loads and parses one schema_avro_value/schema_avro_key file.

    Assumes the file has already passed config-parse-time validation
    (`wiring.config`'s `_validate_avro_schema`, run by `parse_workflow_file`)
    — this doesn't re-validate the schema itself, just (re-)parses it for
    runtime reuse.

    Args:
        schema_ref: Path to the schema file, relative to `base_dir`, or
            None to skip (no schema configured for this topic).
        base_dir: Directory `schema_ref` is resolved relative to (the
            config file's own directory).

    Returns:
        The parsed schema, or None if `schema_ref` is None.
    """
    if schema_ref is None:
        return None
    raw_schema = json.loads((base_dir / schema_ref).read_text())
    parsed = fastavro.parse_schema(raw_schema, named_schemas={})
    # fastavro's stub covers every schema shape (bare "string", a union
    # list, ...), but a schema_avro_value/key file here is always a record
    # (a JSON object) — config-parse-time validation already enforced that.
    assert isinstance(parsed, dict)
    return parsed
