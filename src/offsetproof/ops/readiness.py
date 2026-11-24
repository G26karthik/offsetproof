"""Readiness gates: Kafka, Postgres, topics. Fails loudly, never silently."""

from __future__ import annotations

import os
import sys

import click
from confluent_kafka.admin import AdminClient

from .. import db


@click.command()
@click.option("--bootstrap", default="localhost:29092")
@click.option("--topic", default=os.environ.get("ORDERS_TOPIC", "orders.events"))
@click.option("--dlq-topic", default=os.environ.get("DLQ_TOPIC", "orders.dlq"))
@click.option("--partitions", type=int, default=6)
def main(bootstrap: str, topic: str, dlq_topic: str, partitions: int) -> None:
    admin = AdminClient({"bootstrap.servers": bootstrap})
    metadata = admin.list_topics(timeout=30)
    if metadata is None:
        click.echo("FAIL: no metadata from broker", err=True)
        sys.exit(1)
    for name in (topic, dlq_topic):
        t = metadata.topics.get(name)
        if t is None or t.error is not None:
            click.echo(f"FAIL: topic {name} missing", err=True)
            sys.exit(1)
        click.echo(f"ok: topic {name} partitions={len(t.partitions)}")
    try:
        conn = db.connect()
        with conn.cursor() as cur:
            cur.execute("select count(*) from processed_events")
        conn.close()
    except Exception as exc:  # noqa: BLE001
        click.echo(f"FAIL: postgres unreachable: {exc}", err=True)
        sys.exit(1)
    click.echo("ok: postgres processed_events reachable")
    click.echo("READY")


if __name__ == "__main__":
    main()  # pylint: disable=no-value-for-parameter
