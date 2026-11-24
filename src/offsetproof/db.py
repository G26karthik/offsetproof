"""PostgreSQL access: one transaction per event, idempotent by event_id."""

from __future__ import annotations

import os
from typing import Any

import psycopg

from . import events

APPLY_OK = "applied"
APPLY_DUPLICATE = "duplicate"
APPLY_INVALID_TRANSITION = "invalid_transition"


def dsn() -> str:
    return (
        f"host={os.environ.get('POSTGRES_HOST', 'localhost')} "
        f"port={os.environ.get('POSTGRES_PORT', '5432')} "
        f"dbname={os.environ.get('POSTGRES_DB', 'offsetproof')} "
        f"user={os.environ.get('POSTGRES_USER', 'offsetproof')} "
        f"password={os.environ.get('POSTGRES_PASSWORD', 'offsetproof-local-only')}"
    )


def connect() -> psycopg.Connection:
    return psycopg.connect(dsn())


def apply_event(conn: psycopg.Connection, event: events.OrderEvent) -> str:
    """Insert event_id + apply transition atomically.

    Returns APPLY_OK / APPLY_DUPLICATE / APPLY_INVALID_TRANSITION.
    Raises on transient DB errors (caller retries; nothing was committed).
    """
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO processed_events "
                "(event_id, run_id, order_id, sequence_number, event_type) "
                "VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING "
                "RETURNING event_id",
                (
                    event.event_id,
                    event.run_id,
                    event.order_id,
                    event.sequence_number,
                    event.event_type,
                ),
            )
            if cur.fetchone() is None:
                conn.rollback()
                return APPLY_DUPLICATE
            cur.execute(
                "SELECT status FROM order_projection WHERE order_id = %s FOR UPDATE",
                (event.order_id,),
            )
            row = cur.fetchone()
            current = row[0] if row else events.STATUS_NONE
            nxt = events.next_status(current, event.event_type)
            if nxt is None:
                conn.rollback()
                return APPLY_INVALID_TRANSITION
            cur.execute(
                "INSERT INTO order_projection "
                "(order_id, status, last_sequence, last_event_id) "
                "VALUES (%s, %s, %s, %s) "
                "ON CONFLICT (order_id) DO UPDATE SET status = %s, "
                "last_sequence = %s, last_event_id = %s, "
                "updated_at_utc = now()",
                (
                    event.order_id,
                    nxt,
                    event.sequence_number,
                    event.event_id,
                    nxt,
                    event.sequence_number,
                    event.event_id,
                ),
            )
    return APPLY_OK


def record_dlq(
    conn: psycopg.Connection,
    run_id: str,
    order_id: str | None,
    event_id: str,
    error_class: str,
    raw_payload: dict[str, Any],
) -> None:
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO dlq_records "
                "(event_id, run_id, order_id, error_class, raw_payload) "
                "VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
                (event_id, run_id, order_id, error_class, psycopg.Json(raw_payload)),
            )
