"""Case: schema_avro_value + schema_avro_key on a consumer, schema_avro_value
on a producer.

Only schema_avro_value is enforced at *runtime* on both ends (every
publish/consume checks the payload's shape against it) — schema_avro_key is
validated for shape only, at config-parse time (message keys aren't
Avro-encoded either way; the wire format stays plain JSON, §8).

The two enforcement failure modes differ: a producer-side mismatch (this
job's own output not matching order-validated-value.avro) raises and gets
retried like any handler failure; a consumer-side mismatch (an inbound
message not matching order-input-value.avro) is treated as a poison pill —
logged and silently skipped, the handler never runs. Try triggering with a
payload missing "amount" to see the poison-pill path.

Also sets a header via HandlerResult — schema validation only ever checks
the payload (§8), so it's untouched by whatever's on `headers`.
"""

from __future__ import annotations

from typing import Any

from mira_flow.transport import Message
from mira_flow.wiring import HandlerResult, handler


@handler("order_validate_handler")
def validate_order(message: Message, params: dict[str, Any]) -> HandlerResult:
    return HandlerResult(
        payload={**message.payload, "validated": True},
        headers={"validated-by": "order_validate_handler"},
    )
