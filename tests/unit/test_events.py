"""Unit tests: contract validation, transitions, workload replay (no Docker)."""

import pytest

from offsetproof import events
from offsetproof.events import OrderEvent
from offsetproof.workload import generate_workload


def _base(**over):
    d = {
        "schema_version": 1,
        "event_id": "11111111-1111-1111-1111-111111111111",
        "run_id": "r1",
        "order_id": "order-000001",
        "sequence_number": 0,
        "event_type": "ORDER_CREATED",
        "event_time_utc": "2026-11-24T00:00:00+00:00",
        "payload": {},
    }
    d.update(over)
    return d


@pytest.mark.unit
def test_valid_event_accepted():
    ev, err = events.validate_event(_base())
    assert err is None and isinstance(ev, OrderEvent)


@pytest.mark.unit
@pytest.mark.parametrize(
    "over,err",
    [
        ({"event_type": "NOPE"}, "unknown_event_type"),
        ({"schema_version": 99}, "unsupported_schema_version"),
        ({"sequence_number": -1}, "bad_sequence_number"),
        ({"event_time_utc": "yesterday"}, "bad_timestamp"),
        ({"payload": []}, "bad_payload"),
        ({"order_id": ""}, "bad_order_id"),
    ],
)
def test_invalid_events_rejected(over, err):
    ev, got = events.validate_event(_base(**over))
    assert ev is None and got == err


@pytest.mark.unit
def test_missing_field_named():
    d = _base()
    del d["run_id"]
    ev, err = events.validate_event(d)
    assert ev is None and err == "missing_field:run_id"


@pytest.mark.unit
def test_lifecycle_transitions():
    assert events.next_status("NONE", "ORDER_CREATED") == "CREATED"
    assert events.next_status("CREATED", "PAYMENT_RECORDED") == "PAID"
    assert events.next_status("PAID", "ITEM_PACKED") == "PACKED"
    assert events.next_status("PACKED", "ORDER_SHIPPED") == "SHIPPED"
    assert events.next_status("CREATED", "ORDER_CANCELLED") == "CANCELLED"
    assert events.next_status("CREATED", "ORDER_SHIPPED") is None
    assert events.next_status("SHIPPED", "ORDER_CANCELLED") is None


@pytest.mark.unit
def test_workload_deterministic_and_replay_consistent():
    a = generate_workload(seed=7, n_orders=50, run_id="r")
    b = generate_workload(seed=7, n_orders=50, run_id="r")
    assert [e.event_id for e in a.stream] == [e.event_id for e in b.stream]
    assert a.expected_valid_ids == b.expected_valid_ids
    assert a.expected_valid_ids.isdisjoint(a.expected_dlq_ids)
    # Every sent event is classified exactly once.
    sent = {e.event_id for e in a.stream}
    assert a.expected_valid_ids | a.expected_dlq_ids == sent


@pytest.mark.unit
def test_workload_poison_goes_to_dlq():
    wl = generate_workload(seed=3, n_orders=20, run_id="r", poison_every=5)
    assert wl.expected_dlq_ids, "poison records must be expected in DLQ"
    assert wl.expected_valid_ids.isdisjoint(wl.expected_dlq_ids)
