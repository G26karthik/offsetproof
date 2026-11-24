"""Consumer group with DB-commit-before-offset-commit discipline.

Reads orders.events, applies each event inside one PostgreSQL transaction,
hands permanently invalid events to orders.dlq, and commits a message's
offset only after its DB transaction (or DLQ handoff) is acknowledged.
"""

from __future__ import annotations

import json
import os
import sys
import time
import uuid

import click
from confluent_kafka import Consumer, Producer

from . import db, events, metrics

CRASH_AFTER_DB_COMMIT = os.environ.get("OFFSETPROOF_CRASH_AFTER_DB_COMMIT") == "1"
DB_DELAY_SECONDS = float(os.environ.get("OFFSETPROOF_DB_DELAY_SECONDS", "0"))
TRANSIENT_RETRY_LIMIT = int(os.environ.get("OFFSETPROOF_RETRY_LIMIT", "50"))


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
def main(bootstrap: str, group_id: str, topic: str, dlq_topic: str,
         metrics_port: int) -> None:
    metrics.serve(metrics_port)
    consumer = make_consumer(bootstrap, group_id)
    dlq = Producer(
        {
            "bootstrap.servers": bootstrap,
            "acks": "all",
            "enable.idempotence": True,
            "client.id": "offsetproof-dlq",
        }
    )
    conn = db.connect()

    def on_assign(c, partitions) -> None:
        metrics.REBALANCES.labels(group_id=group_id).inc()
        log_record("info", event="assigned",
                   partitions=[(p.topic, p.partition) for p in partitions])

    def on_revoke(c, partitions) -> None:
        log_record("info", event="revoked",
                   partitions=[(p.topic, p.partition) for p in partitions])

    consumer.subscribe([topic], on_assign=on_assign, on_revoke=on_revoke)
    log_record("info", event="started", group=group_id, topic=topic)

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                log_record("warn", event="kafka_error", error=str(msg.error()))
                continue
            kind, event, detail = classify(msg.value())
            run_id = event.run_id if event else ""
            if kind == "valid" and event is not None:
                if DB_DELAY_SECONDS:  # F2 slow-sink hook (evaluator-side only)
                    time.sleep(DB_DELAY_SECONDS)
                retries = 0
                while True:
                    try:
                        outcome = db.apply_event(conn, event)
                        break
                    except Exception as exc:  # noqa: BLE001 - transient path
                        retries += 1
                        log_record("warn", event="transient_retry",
                                   attempt=retries, error=str(exc))
                        if retries > TRANSIENT_RETRY_LIMIT:
                            log_record("error", event="retry_budget_exhausted")
                            raise
                        time.sleep(min(2.0 ** retries, 30.0))
                if CRASH_AFTER_DB_COMMIT and outcome == db.APPLY_OK:
                    os._exit(137)  # noqa: SLOS - F6 deterministic test hook
                if outcome == db.APPLY_INVALID_TRANSITION:
                    kind, detail = "invalid", f"invalid_transition:{event.event_type}"
                else:
                    metrics.CONSUMED.labels(run_id=run_id, outcome=outcome).inc()
            if kind != "valid":
                raw_payload: dict = {}
                if event is not None:
                    raw_payload = event.to_dict()
                else:
                    try:
                        raw_payload = json.loads(msg.value().decode("utf-8"))
                    except (ValueError, UnicodeDecodeError):
                        raw_payload = {"_undecodable": True}
                dlq_id = (event.event_id if event else None) or (
                    f"undecodable-{msg.partition()}-{msg.offset()}"
                )
                db.record_dlq(conn, run_id,
                              event.order_id if event else None,
                              dlq_id, detail or kind, raw_payload)
                dlq.produce(dlq_topic, key=msg.key(),
                            value=json.dumps(raw_payload).encode())
                dlq.flush(60)
                metrics.DLQ_RECORDED.labels(run_id=run_id).inc()
                if CRASH_AFTER_DB_COMMIT:
                    os._exit(137)  # noqa: SLOS - F6 deterministic test hook
            consumer.commit(message=msg, asynchronous=False)
            log_record("info", event="committed", partition=msg.partition(),
                       offset=msg.offset(), kind=kind, detail=detail)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    main()  # pylint: disable=no-value-for-parameter
