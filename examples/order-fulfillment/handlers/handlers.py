"""Case: a multi-job order-fulfillment flow — 8 jobs combining every routing
shape the engine supports:

    receive_order ──invalid──────────────────────────────┐
         │                                               ▼
         ▼                                        notify_rejection (terminal)
    fraud_check ──risk_score >= 80──► manual_review ──rejected──┘
         │                                 │
         │ (low risk)                      │ approved
         ▼                                 ▼
    charge_payment ◄───────────────────────┘    (merge: 2 producers -> 1 topic)
     (retries: 2)
         ├──shipping == "express"──► ship_express ──┐
         └──otherwise─────────────► ship_standard ──┤   (merge again)
                                                    ▼
                                           notify_customer (terminal)

- Branching: every non-terminal job has 2 routes, first match wins (§5).
- Merging: `ex-order-payment`, `ex-order-notify` and `ex-order-rejected` each
  have two upstream jobs publishing to them (only *consuming* a topic must be
  unique per job, producing to it isn't).
- Retries: `charge_payment` simulates a payment gateway, picked by the
  order's `gateway` field:
    * "ok"     — succeeds first time.
    * "flaky"  — fails attempt 1, succeeds on the automatic retry.
    * "outage" — fails every automatic attempt (1 + 2 retries), so the task
      ends FAILED and so does the run. Then hit **Retry** on that task in the
      UI (or POST /runs/{id}/tasks/{task_run_id}/retry): the gateway has
      "recovered" by then, so the run resumes and finishes SUCCEEDED.
- Every handler sleeps `params.work_seconds` so runs stay visibly RUNNING in
  the UI's grid/graph for a few seconds.

Attempt counts are kept in memory per workflow_run_id (falling back to
message.id outside the ledgered orchestrator), so restarting the worker
resets them — fine for a demo.
"""

from __future__ import annotations

import time
from typing import Any

from mira_flow.transport import Message
from mira_flow.wiring import HandlerResult, handler

_RISKY_COUNTRIES = {"XX", "ZZ"}
_DISPOSABLE_EMAIL_DOMAIN = "@tempmail.test"

_payment_attempts: dict[str, int] = {}


def _work(params: dict[str, Any]) -> None:
    time.sleep(float(params.get("work_seconds", 0)))


@handler("order_receive_handler")
def receive_order(message: Message, params: dict[str, Any]) -> HandlerResult:
    """Validates the raw order and computes its total."""
    _work(params)
    order = message.payload
    items = order.get("items") or []
    errors: list[str] = []
    if not order.get("order_id"):
        errors.append("missing order_id")
    if not items:
        errors.append("order has no items")
    if any(i.get("qty", 0) <= 0 or i.get("price", 0) < 0 for i in items):
        errors.append("invalid item qty/price")

    total = round(sum(i.get("qty", 0) * i.get("price", 0) for i in items), 2)
    return HandlerResult(
        payload={
            **order,
            "total": total,
            "item_count": sum(i.get("qty", 0) for i in items),
            "valid": not errors,
            "errors": errors,
            "shipping": order.get("shipping", "standard"),
            "gateway": order.get("gateway", "ok"),
        },
        headers={"order-id": str(order.get("order_id", ""))},
    )


@handler("order_fraud_check_handler")
def fraud_check(message: Message, params: dict[str, Any]) -> dict[str, Any]:
    """Scores the order; >= 80 is routed to manual_review instead of payment."""
    _work(params)
    order = message.payload
    customer = order.get("customer", {})
    reasons: list[str] = []
    score = 10
    if order["total"] > params.get("high_value_threshold", 1000):
        score += 50
        reasons.append("high value")
    if customer.get("country") in _RISKY_COUNTRIES:
        score += 30
        reasons.append("risky country")
    if str(customer.get("email", "")).endswith(_DISPOSABLE_EMAIL_DOMAIN):
        score += 20
        reasons.append("disposable email")
    return {**order, "risk_score": score, "risk_reasons": reasons}


@handler("order_manual_review_handler")
def manual_review(message: Message, params: dict[str, Any]) -> dict[str, Any]:
    """Simulated human review: approves unless the order is huge or the email
    is disposable."""
    _work(params)
    order = message.payload
    email = str(order.get("customer", {}).get("email", ""))
    too_big = order["total"] > params.get("max_auto_approve_total", 5000)
    disposable = email.endswith(_DISPOSABLE_EMAIL_DOMAIN)
    approved = not (too_big or disposable)
    reason = (
        "approved by reviewer"
        if approved
        else ("total above review limit" if too_big else "disposable email")
    )
    return {
        **order,
        "approved": approved,
        "review": {"reviewer": "demo-bot", "reason": reason},
        # notify_rejection reads `errors`, same shape receive_order uses.
        "errors": [] if approved else [f"rejected in review: {reason}"],
    }


class PaymentGatewayError(RuntimeError):
    """Simulated transient gateway failure."""


@handler("order_charge_payment_handler")
def charge_payment(message: Message, params: dict[str, Any]) -> dict[str, Any]:
    """Charges the card through a simulated gateway (see module docstring)."""
    _work(params)
    order = message.payload
    key = message.workflow_run_id or message.id
    attempt = _payment_attempts.get(key, 0) + 1
    _payment_attempts[key] = attempt

    gateway = order.get("gateway", "ok")
    # "outage" outlasts the job's automatic retries (1 + 2), so only a
    # manual retry (attempt 4+) gets through.
    fail_until = {"ok": 1, "flaky": 2, "outage": 4}.get(gateway, 1)
    if attempt < fail_until:
        raise PaymentGatewayError(
            f"gateway {gateway!r} unavailable (attempt {attempt}, "
            f"succeeds from attempt {fail_until})"
        )
    return {
        **order,
        "payment": {
            "status": "captured",
            "amount": order["total"],
            "transaction_id": f"txn-{order['order_id']}-{attempt}",
            "attempts": attempt,
        },
    }


def _ship(message: Message, params: dict[str, Any]) -> dict[str, Any]:
    _work(params)
    order = message.payload
    return {
        **order,
        "shipment": {
            "carrier": params["carrier"],
            "tracking_number": f"{params['carrier'][:3].upper()}-{order['order_id']}",
            "eta_days": params["eta_days"],
        },
    }


@handler("order_ship_standard_handler")
def ship_standard(message: Message, params: dict[str, Any]) -> dict[str, Any]:
    return _ship(message, params)


@handler("order_ship_express_handler")
def ship_express(message: Message, params: dict[str, Any]) -> dict[str, Any]:
    return _ship(message, params)


@handler("order_notify_customer_handler")
def notify_customer(message: Message, params: dict[str, Any]) -> dict[str, Any]:
    """Terminal: the returned payload becomes the run's `output`."""
    _work(params)
    order = message.payload
    email = order.get("customer", {}).get("email")
    shipment = order["shipment"]
    print(
        f"[order-fulfillment] {order['order_id']}: confirmed to {email}, "
        f"{shipment['carrier']} {shipment['tracking_number']} (ETA {shipment['eta_days']}d)"
    )
    return {
        "order_id": order["order_id"],
        "outcome": "fulfilled",
        "total": order["total"],
        "payment": order["payment"],
        "shipment": shipment,
        "risk_score": order.get("risk_score"),
        "reviewed": "review" in order,
    }


@handler("order_notify_rejection_handler")
def notify_rejection(message: Message, params: dict[str, Any]) -> dict[str, Any]:
    """Terminal: tells the customer why their order was refused."""
    _work(params)
    order = message.payload
    print(f"[order-fulfillment] {order.get('order_id')}: rejected — {order.get('errors')}")
    return {
        "order_id": order.get("order_id"),
        "outcome": "rejected",
        "errors": order.get("errors", []),
        "risk_score": order.get("risk_score"),
    }
