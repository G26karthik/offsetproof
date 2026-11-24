"""OffsetProof event contract and order lifecycle state machine.

Stdlib only — unit-testable without Kafka, Postgres, or third-party deps.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

SCHEMA_VERSION = 1

ORDER_CREATED = "ORDER_CREATED"
PAYMENT_RECORDED = "PAYMENT_RECORDED"
ITEM_PACKED = "ITEM_PACKED"
ORDER_SHIPPED = "ORDER_SHIPPED"
ORDER_CANCELLED = "ORDER_CANCELLED"

EVENT_TYPES = frozenset(
    {ORDER_CREATED, PAYMENT_RECORDED, ITEM_PACKED, ORDER_SHIPPED, ORDER_CANCELLED}
)

# Synthetic payment-ledger profile: money-movement lifecycle for the same
# pipeline. No real money, no PII, no employer data — the point is that
# ledger posting has the same at-least-once + idempotent-apply shape as
# order projection, with stricter reconciliation (every cent accounted for).
PAYMENT_INITIATED = "PAYMENT_INITIATED"
PAYMENT_AUTHORIZED = "PAYMENT_AUTHORIZED"
PAYMENT_CAPTURED = "PAYMENT_CAPTURED"
PAYMENT_SETTLED = "PAYMENT_SETTLED"
PAYMENT_REVERSED = "PAYMENT_REVERSED"
PAYMENT_REFUNDED = "PAYMENT_REFUNDED"

LEDGER_EVENT_TYPES = frozenset(
    {
        PAYMENT_INITIATED,
        PAYMENT_AUTHORIZED,
        PAYMENT_CAPTURED,
        PAYMENT_SETTLED,
        PAYMENT_REVERSED,
        PAYMENT_REFUNDED,
    }
)

EVENT_TYPES = EVENT_TYPES | LEDGER_EVENT_TYPES

# Status of an order that has no applied events yet.
STATUS_NONE = "NONE"
STATUS_CREATED = "CREATED"
STATUS_PAID = "PAID"
STATUS_PACKED = "PACKED"
STATUS_SHIPPED = "SHIPPED"
STATUS_CANCELLED = "CANCELLED"

# current status -> {event_type: next status}. Absent entry = invalid transition.
TRANSITIONS: dict[str, dict[str, str]] = {
    STATUS_NONE: {ORDER_CREATED: STATUS_CREATED},
    STATUS_CREATED: {
        PAYMENT_RECORDED: STATUS_PAID,
        ORDER_CANCELLED: STATUS_CANCELLED,
    },
    STATUS_PAID: {
        ITEM_PACKED: STATUS_PACKED,
        ORDER_CANCELLED: STATUS_CANCELLED,
    },
    STATUS_PACKED: {ORDER_SHIPPED: STATUS_SHIPPED},
    STATUS_SHIPPED: {},
    STATUS_CANCELLED: {},
}

REQUIRED_FIELDS = (
    "schema_version",
    "event_id",
    "run_id",
    "order_id",
    "sequence_number",
    "event_type",
    "event_time_utc",
    "payload",
)


@dataclass(frozen=True)
class OrderEvent:
    schema_version: int
    event_id: str
    run_id: str
    order_id: str
    sequence_number: int
    event_type: str
    event_time_utc: str
    payload: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "event_id": self.event_id,
            "run_id": self.run_id,
            "order_id": self.order_id,
            "sequence_number": self.sequence_number,
            "event_type": self.event_type,
            "event_time_utc": self.event_time_utc,
            "payload": self.payload,
        }


LEDGER_TRANSITIONS: dict[str, dict[str, str]] = {
    STATUS_NONE: {PAYMENT_INITIATED: "INITIATED"},
    "INITIATED": {
        PAYMENT_AUTHORIZED: "AUTHORIZED",
        PAYMENT_REVERSED: "REVERSED",
    },
    "AUTHORIZED": {
        PAYMENT_CAPTURED: "CAPTURED",
        PAYMENT_REVERSED: "REVERSED",
    },
    "CAPTURED": {
        PAYMENT_SETTLED: "SETTLED",
        PAYMENT_REVERSED: "REVERSED",
    },
    "SETTLED": {PAYMENT_REFUNDED: "REFUNDED"},
    "REVERSED": {},
    "REFUNDED": {},
}

PROFILES: dict[str, dict[str, dict[str, str]]] = {
    "orders": TRANSITIONS,
    "ledger": LEDGER_TRANSITIONS,
}


def next_status(
    current: str, event_type: str, table: dict[str, dict[str, str]] | None = None
) -> str | None:
    """Return the status after applying event_type, or None if invalid."""
    return (table or TRANSITIONS).get(current, {}).get(event_type)


def validate_event(raw: Any) -> tuple[OrderEvent | None, str | None]:
    """Validate a decoded JSON value. Returns (event, None) or (None, class)."""
    if not isinstance(raw, dict):
        return None, "not_an_object"
    for field in REQUIRED_FIELDS:
        if field not in raw:
            return None, f"missing_field:{field}"
    if raw["schema_version"] != SCHEMA_VERSION:
        return None, "unsupported_schema_version"
    try:
        uuid.UUID(str(raw["event_id"]))
    except (ValueError, AttributeError, TypeError):
        return None, "bad_event_id"
    if not isinstance(raw["order_id"], str) or not raw["order_id"]:
        return None, "bad_order_id"
    if (
        not isinstance(raw["sequence_number"], int)
        or isinstance(raw["sequence_number"], bool)
        or raw["sequence_number"] < 0
    ):
        return None, "bad_sequence_number"
    if raw["event_type"] not in EVENT_TYPES:
        return None, "unknown_event_type"
    try:
        ts = datetime.fromisoformat(str(raw["event_time_utc"]).replace("Z", "+00:00"))
        if ts.tzinfo is None:
            return None, "naive_timestamp"
    except (ValueError, TypeError):
        return None, "bad_timestamp"
    if not isinstance(raw["payload"], dict):
        return None, "bad_payload"
    if not isinstance(raw["run_id"], str) or not raw["run_id"]:
        return None, "bad_run_id"
    return (
        OrderEvent(
            schema_version=raw["schema_version"],
            event_id=str(raw["event_id"]),
            run_id=str(raw["run_id"]),
            order_id=raw["order_id"],
            sequence_number=raw["sequence_number"],
            event_type=raw["event_type"],
            event_time_utc=str(raw["event_time_utc"]),
            payload=raw["payload"],
        ),
        None,
    )


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()
