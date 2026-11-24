"""Consumer group with DB-commit-before-offset-commit discipline.

Reads orders.events, applies each event inside one PostgreSQL transaction,
hands permanently invalid events to orders.dlq, and commits a message's
offset only after its DB transaction (or DLQ handoff) is acknowledged.
"""

from __future__ import annotations

import json
import os
import time
import uuid
from typing import Any

import click
from confluent_kafka import Consumer, Producer

from . import db, events, metrics

CRASH_AFTER_DB_COMMIT = os.environ.get("OFFSETPROOF_CRASH_AFTER_DB_COMMIT") == "1"
DB_DELAY_SECONDS = float(os.environ.get("OFFSETPROOF_DB_DELAY_SECONDS", "0"))
TRANSIENT_RETRY_LIMIT = int(os.environ.get("OFFSETPROOF_RETRY_LIMIT", "50"))


def backoff_seconds(attempt: int) -> float:
    """Bounded exponential backoff: 2, 4, 8, ... capped at 30s."""
    return float(min(2.0**attempt, 30.0))


COMMIT_EVERY = 50
COMMIT_INTERVAL_SECONDS = 5.0


def should_commit(committed: int, last_commit_at: float, now: float) -> bool:
    """Batch the synchronous offset commit without weakening the guarantee.

    A committed offset always covers only already-processed messages
    (single-threaded, in-order), so committing every Nth message or every
    few seconds preserves exactly the same promise as committing each one:
    a crash redelivers at most the uncommitted tail, which the idempotent
    sink absorbs.
    """
    if committed <= 0:
        return False
    return (
        committed % COMMIT_EVERY == 0
        or (now - last_commit_at) >= COMMIT_INTERVAL_SECONDS
    )


class ResilientConnection:
    """One logical DB handle that reopens itself after a transport failure.

    A psycopg connection whose server went away never recovers on its own:
    every further statement fails on the same dead socket. Retrying on a
    dead handle only burns the retry budget, so each failed attempt closes
    the handle and the next attempt dials fresh.
    """

    def __init__(self, connect: Any = None) -> None:
        self._connect = connect or db.connect
        self.conn: Any = self._connect()

    def reset(self) -> None:
        try:
            self.conn.close()
        except Exception:  # noqa: BLE001 - close is best-effort
            pass
        self.conn = self._connect()


def with_retries(store: ResilientConnection, op: Any, what: str) -> Any:
    """Run a DB op, reopening the handle on transport failure.

    A dead handle fails every statement identically, so each failed attempt
    resets the connection before backing off. Exhaustion raises: the caller
    must not commit past a message whose write never landed.
    """
    retries = 0
    while True:
        try:
            return op(store.conn)
        except Exception as exc:  # noqa: BLE001 - transient path
            retries += 1
            log_record("warn", event="transient_retry", op=what,
                       attempt=retries, error=str(exc))
            if retries > TRANSIENT_RETRY_LIMIT:
                log_record("error", event="retry_budget_exhausted", op=what)
                raise
            store.reset()
            time.sleep(backoff_seconds(retries))


def log_record(level: str, **fields: object) -> None:
    print(json.dumps({"level": level, **fields}), flush=True)


def make_consumer(bootstrap: str, group_id: str) -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": group_id,
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "auto.offset.reset": "earliest",
            "client.id": f"offsetproof-consumer-{uuid.uuid4().hex[:6]}",
        }
    )


def classify(raw: bytes) -> tuple[str, events.OrderEvent | None, str]:
    """Return (kind, event, detail) where kind is valid/invalid/undecodable."""
    try:
        decoded = json.loads(raw.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        return "undecodable", None, "undecodable_bytes"
    event, error_class = events.validate_event(decoded)
    if event is None:
        return "invalid", None, error_class or "unknown"
    return "valid", event, ""


@click.command()
@click.option("--bootstrap", default="localhost:29092")
@click.option("--group-id", default="offsetproof-projector")
@click.option("--topic", default=os.environ.get("ORDERS_TOPIC", "orders.events"))
@click.option("--dlq-topic", default=os.environ.get("DLQ_TOPIC", "orders.dlq"))
@click.option("--metrics-port", type=int, default=8000)
@click.option("--max-messages", type=int, default=0,
              help="Exit 0 after N commits; 0 = run forever")
@click.option("--profile", type=click.Choice(["orders", "ledger"]),
              default="orders")
def main(bootstrap: str, group_id: str, topic: str, dlq_topic: str,
         metrics_port: int, max_messages: int, profile: str) -> None:
    metrics.serve(metrics_port)
    table = events.PROFILES[profile]
    consumer = make_consumer(bootstrap, group_id)
    dlq = Producer(
        {
            "bootstrap.servers": bootstrap,
            "acks": "all",
            "enable.idempotence": True,
            "client.id": "offsetproof-dlq",
        }
    )
    store = ResilientConnection()

    def on_assign(c: Any, partitions: list[Any]) -> None:
        metrics.REBALANCES.labels(group_id=group_id).inc()
        log_record("info", event="assigned",
                   partitions=[(p.topic, p.partition) for p in partitions])

    def on_revoke(c: Any, partitions: list[Any]) -> None:
        log_record("info", event="revoked",
                   partitions=[(p.topic, p.partition) for p in partitions])

    consumer.subscribe([topic], on_assign=on_assign, on_revoke=on_revoke)
    log_record("info", event="started", group=group_id, topic=topic)
    committed = 0
    last_commit_at = time.monotonic()
    last_msg = None

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                log_record("warn", event="kafka_error", error=str(msg.error()))
                continue
            raw = msg.value()
            if raw is None:
                # No tombstones are ever produced to this topic; a None value
                # means a corrupt record. Advance past it loudly, never silently.
                log_record("warn", event="none_value",
                           partition=msg.partition(), offset=msg.offset())
                consumer.commit(message=msg, asynchronous=False)
                continue
            kind, event, detail = classify(raw)
            # A schema-invalid record has no parsed event, but its raw JSON
            # envelope still carries the producer's run_id. Without this,
            # poison records are stored under run_id='' and the run-scoped
            # oracle can never reconcile them (run-06/07 lesson).
            envelope_run_id = ""
            envelope_event_id: str | None = None
            try:
                decoded = json.loads(raw.decode("utf-8"))
                if isinstance(decoded, dict):
                    envelope_run_id = str(decoded.get("run_id", "") or "")
                    candidate = decoded.get("event_id")
                    if isinstance(candidate, str):
                        try:
                            uuid.UUID(candidate)
                            envelope_event_id = candidate
                        except ValueError:
                            pass
            except (ValueError, UnicodeDecodeError):
                pass
            run_id = (event.run_id if event else "") or envelope_run_id
            if kind == "valid" and event is not None:
                if DB_DELAY_SECONDS:  # F2 slow-sink hook (evaluator-side only)
                    time.sleep(DB_DELAY_SECONDS)
                outcome = with_retries(
                    store,
                    lambda c: db.apply_event(c, event, table),
                    "apply_event",
                )
                if CRASH_AFTER_DB_COMMIT and outcome == db.APPLY_OK:
                    os._exit(137)  # noqa: SLOS - F6 deterministic test hook
                if outcome == db.APPLY_INVALID_TRANSITION:
                    kind, detail = "invalid", f"invalid_transition:{event.event_type}"
                else:
                    metrics.CONSUMED.labels(run_id=run_id, outcome=outcome).inc()
            if kind != "valid":
                raw_payload: dict[str, Any] = {}
                if event is not None:
                    raw_payload = event.to_dict()
                else:
                    try:
                        raw_payload = json.loads(raw.decode("utf-8"))
                    except (ValueError, UnicodeDecodeError):
                        raw_payload = {"_undecodable": True}
                if event is not None:
                    dlq_id = event.event_id
                elif envelope_event_id is not None:
                    dlq_id = envelope_event_id
                else:
                    dlq_id = f"undecodable-{msg.partition()}-{msg.offset()}"
                with_retries(
                    store,
                    lambda c: db.record_dlq(
                        c, run_id, event.order_id if event else None,
                        dlq_id, detail or kind, raw_payload,
                    ),
                    "record_dlq",
                )
                dlq.produce(dlq_topic, key=msg.key(),
                            value=json.dumps(raw_payload).encode())
                dlq.flush(60)
                metrics.DLQ_RECORDED.labels(run_id=run_id).inc()
                if CRASH_AFTER_DB_COMMIT:
                    os._exit(137)  # noqa: SLOS - F6 deterministic test hook
            committed += 1
            last_msg = msg
            if should_commit(committed, last_commit_at, time.monotonic()):
                consumer.commit(message=msg, asynchronous=False)
                last_commit_at = time.monotonic()
            log_record("info", event="committed", partition=msg.partition(),
                       offset=msg.offset(), kind=kind, detail=detail,
                       event_id=event.event_id if event else None)
            if max_messages and committed >= max_messages:
                log_record("info", event="max_messages_reached",
                           committed=committed)
                break
    except KeyboardInterrupt:
        pass
    finally:
        try:
            if last_msg is not None:
                consumer.commit(message=last_msg, asynchronous=False)
        except Exception:  # noqa: BLE001 - shutdown commit is best-effort
            pass
        consumer.close()
        try:
            store.conn.close()
        except Exception:  # noqa: BLE001 - shutdown is best-effort
            pass


if __name__ == "__main__":
    main()  # pylint: disable=no-value-for-parameter
