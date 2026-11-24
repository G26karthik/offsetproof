"""DB sink semantics against real Postgres (needs Docker daemon).

Guards the exact bug real data caught on 2026-11-24: a rejected event must
leave no row in processed_events, or the oracle's phantom check degrades
into noise.
"""

import uuid

import pytest

from offsetproof import db, events

pytestmark = pytest.mark.integration


def _event(order_id: str, seq: int, etype: str) -> events.OrderEvent:
    return events.OrderEvent(
        schema_version=1,
        event_id=str(uuid.uuid4()),
        run_id="test-run",
        order_id=order_id,
        sequence_number=seq,
        event_type=etype,
        event_time_utc="2026-11-24T00:00:00+00:00",
        payload={},
    )


@pytest.fixture()
def conn():
    c = db.connect()
    c.execute("TRUNCATE processed_events, order_projection, dlq_records")
    c.commit()
    yield c
    c.execute("TRUNCATE processed_events, order_projection, dlq_records")
    c.commit()
    c.close()


def test_invalid_transition_leaves_no_row(conn):
    outcome = db.apply_event(
        conn, _event("ghost-1", 0, events.ORDER_CANCELLED)
    )
    assert outcome == db.APPLY_INVALID_TRANSITION
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM processed_events")
        assert cur.fetchone()[0] == 0


def test_duplicate_apply_is_single_effect(conn):
    ev = _event("o-1", 0, events.ORDER_CREATED)
    assert db.apply_event(conn, ev) == db.APPLY_OK
    assert db.apply_event(conn, ev) == db.APPLY_DUPLICATE
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM processed_events")
        assert cur.fetchone()[0] == 1
        cur.execute("SELECT status FROM order_projection WHERE order_id = 'o-1'")
        assert cur.fetchone()[0] == "CREATED"


def test_ledger_double_settle_rejected_without_trace(conn):
    table = events.LEDGER_TRANSITIONS
    base = _event("pay-1", 0, events.PAYMENT_INITIATED)
    assert db.apply_event(conn, base, table) == db.APPLY_OK
    assert db.apply_event(conn, _event("pay-1", 1, events.PAYMENT_AUTHORIZED), table) == db.APPLY_OK
    assert db.apply_event(conn, _event("pay-1", 2, events.PAYMENT_CAPTURED), table) == db.APPLY_OK
    assert db.apply_event(conn, _event("pay-1", 3, events.PAYMENT_SETTLED), table) == db.APPLY_OK
    assert db.apply_event(conn, _event("pay-1", 4, events.PAYMENT_SETTLED), table) == db.APPLY_INVALID_TRANSITION
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM processed_events")
        assert cur.fetchone()[0] == 4
        cur.execute("SELECT status FROM order_projection WHERE order_id = 'pay-1'")
        assert cur.fetchone()[0] == "SETTLED"
