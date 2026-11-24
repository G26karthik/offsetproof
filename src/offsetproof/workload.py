"""Deterministic synthetic order-workload generator (stdlib only).

Builds the exact stream to SEND (valid + poison interleaved) and derives
the expected outcome by replaying that stream through the state machine —
the same replay the independent oracle performs. Expected data is ground
truth and must never be visible to the diagnosis path.
"""

from __future__ import annotations

import random
import uuid
from dataclasses import dataclass, field

from .events import (
    ITEM_PACKED,
    ORDER_CANCELLED,
    ORDER_CREATED,
    ORDER_SHIPPED,
    PAYMENT_RECORDED,
    STATUS_CANCELLED,
    STATUS_CREATED,
    STATUS_NONE,
    OrderEvent,
    next_status,
    utc_now_iso,
)

FULL_LIFECYCLE = [ORDER_CREATED, PAYMENT_RECORDED, ITEM_PACKED, ORDER_SHIPPED]
CANCELLED_LIFECYCLE = [ORDER_CREATED, PAYMENT_RECORDED, ORDER_CANCELLED]

_FINAL_STATUS = {
    ORDER_CREATED: STATUS_CREATED,
    PAYMENT_RECORDED: "PAID",
    ITEM_PACKED: "PACKED",
    ORDER_SHIPPED: "SHIPPED",
    ORDER_CANCELLED: STATUS_CANCELLED,
}


@dataclass
class Workload:
    run_id: str
    seed: int
    # Every record the producer will send, in send order. Poison records are
    # structurally invalid (bad schema_version / unknown type).
    stream: list[OrderEvent] = field(default_factory=list)
    # event_ids the oracle expects to find in processed_events.
    expected_valid_ids: set[str] = field(default_factory=set)
    # event_ids the oracle expects to find (distinct) in the DLQ.
    expected_dlq_ids: set[str] = field(default_factory=set)
    # order_id -> final status after replaying the stream.
    expected_final: dict[str, str] = field(default_factory=dict)


def generate_workload(
    seed: int,
    n_orders: int,
    run_id: str,
    skew_hot_fraction: float = 0.0,
    poison_every: int = 0,
    cancel_fraction: float = 0.1,
) -> Workload:
    """Build a deterministic interleaved stream plus replayed expectations.

    skew_hot_fraction: share of orders mapped to order id "hot-0", creating
    key skew toward one actual partition (routing verified, not assumed).
    poison_every: every Nth sent record is replaced by an invalid record
    with a fresh event_id.
    """
    rng = random.Random(seed)
    per_order_types: dict[str, list[str]] = {}
    for i in range(n_orders):
        if skew_hot_fraction > 0 and rng.random() < skew_hot_fraction:
            order_id = "hot-0"
        else:
            order_id = f"order-{i:06d}"
        lifecycle = (
            CANCELLED_LIFECYCLE if rng.random() < cancel_fraction else FULL_LIFECYCLE
        )
        per_order_types.setdefault(order_id, []).extend(lifecycle)

    # Per-order queues of valid events with deterministic ids.
    queues: dict[str, list[OrderEvent]] = {}
    for order_id, types in per_order_types.items():
        evs = []
        for seq, etype in enumerate(types):
            evs.append(
                OrderEvent(
                    schema_version=1,
                    event_id=str(
                        uuid.uuid5(uuid.NAMESPACE_URL, f"{run_id}:{order_id}:{seq}")
                    ),
                    run_id=run_id,
                    order_id=order_id,
                    sequence_number=seq,
                    event_type=etype,
                    event_time_utc=utc_now_iso(),
                    payload={"pos": seq},
                )
            )
        queues[order_id] = evs

    interleaved: list[OrderEvent] = []
    pending = {k: list(v) for k, v in queues.items()}
    while any(pending.values()):
        for order_id in sorted(pending):
            if pending[order_id]:
                interleaved.append(pending[order_id].pop(0))

    stream: list[OrderEvent] = []
    sent = 0
    for ev in interleaved:
        sent += 1
        if poison_every > 0 and sent % poison_every == 0:
            stream.append(
                OrderEvent(
                    schema_version=99,
                    event_id=str(
                        uuid.uuid5(uuid.NAMESPACE_URL, f"{run_id}:poison:{sent}")
                    ),
                    run_id=run_id,
                    order_id=ev.order_id,
                    sequence_number=ev.sequence_number,
                    event_type="BOGUS_TYPE",
                    event_time_utc=ev.event_time_utc,
                    payload={},
                )
            )
        else:
            stream.append(ev)

    # Replay the send stream to derive expectations.
    wl = Workload(run_id=run_id, seed=seed, stream=stream)
    state: dict[str, str] = {}
    for ev in stream:
        if ev.schema_version != 1 or ev.event_type not in _FINAL_STATUS:
            wl.expected_dlq_ids.add(ev.event_id)
            continue
        current = state.get(ev.order_id, STATUS_NONE)
        nxt = next_status(current, ev.event_type)
        if nxt is None:
            wl.expected_dlq_ids.add(ev.event_id)
            continue
        wl.expected_valid_ids.add(ev.event_id)
        state[ev.order_id] = nxt
    wl.expected_final = state
    return wl
