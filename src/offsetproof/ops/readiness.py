"""Readiness gates: broker reachable, topics ensured, Postgres reachable."""

from __future__ import annotations

import os
import sys

import click
from confluent_kafka.admin import AdminClient, NewTopic  # type: ignore[attr-defined]

from .. import db


@click.command()
@click.option("--bootstrap", default="localhost:29092")
@click.option("--topic", default=os.environ.get("ORDERS_TOPIC", "orders.events"))
@click.option("--dlq-topic", default=os.environ.get("DLQ_TOPIC", "orders.dlq"))
@click.option("--partitions", type=int, default=6)
def main(bootstrap: str, topic: str, dlq_topic: str, partitions: int) -> None:
    admin = AdminClient({"bootstrap.servers": bootstrap})
    try:
        existing = set(admin.list_topics(timeout=30).topics)
    except Exception as exc:  # noqa: BLE001
        click.echo(f"FAIL: no metadata from broker: {exc}", err=True)
        sys.exit(1)
    wanted = {topic: partitions, dlq_topic: 1}
    missing = [t for t in wanted if t not in existing]
    if missing:
        futures = admin.create_topics(
            [NewTopic(t, wanted[t], 1) for t in missing]
        )
        for t, fut in futures.items():
            try:
                fut.result(timeout=60)
            except Exception as exc:  # noqa: BLE001
                click.echo(f"FAIL: cannot create topic {t}: {exc}", err=True)
                sys.exit(1)
            click.echo(f"created topic {t} partitions={wanted[t]}")
    for name in wanted:
        click.echo(f"ok: topic {name}")
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
