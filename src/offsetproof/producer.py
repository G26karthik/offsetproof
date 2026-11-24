"""Seeded producer: workload -> Kafka with acks=all + ack manifest."""

from __future__ import annotations

import json
import os
import time
import uuid
from pathlib import Path
from typing import Any

import click
from confluent_kafka import Producer

from . import metrics, workload as workload_mod


def make_producer(bootstrap: str) -> Producer:
    return Producer(
        {
            "bootstrap.servers": bootstrap,
            "acks": "all",
            "enable.idempotence": True,
            "retries": 10,
            "compression.type": "lz4",
            "client.id": "offsetproof-producer",
        }
    )


@click.command()
@click.option("--seed", type=int, default=42)
@click.option("--orders", type=int, default=2500)
@click.option("--skew-hot-fraction", type=float, default=0.0)
@click.option("--poison-every", type=int, default=0)
@click.option("--rate", type=float, default=0.0, help="Max msgs/sec, 0 = unthrottled")
@click.option("--topic", default=os.environ.get("ORDERS_TOPIC", "orders.events"))
@click.option("--bootstrap", default="localhost:29092")
@click.option("--out-dir", default="results/latest")
def main(seed: int, orders: int, skew_hot_fraction: float, poison_every: int,
         rate: float, topic: str, bootstrap: str, out_dir: str) -> None:
    run_id = f"run-{uuid.uuid4().hex[:12]}"
    wl = workload_mod.generate_workload(
        seed=seed,
        n_orders=orders,
        run_id=run_id,
        skew_hot_fraction=skew_hot_fraction,
        poison_every=poison_every,
    )
    producer = make_producer(bootstrap)
    acked: list[str] = []
    failed: list[dict[str, Any]] = []

    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    interval = 1.0 / rate if rate > 0 else 0.0
    for ev in wl.stream:
        payload = json.dumps(ev.to_dict()).encode()

        # Default-arg binding avoids the late-binding closure trap.
        def _cb(err: Any, msg: Any, eid: str = ev.event_id) -> None:
            if err is None:
                acked.append(eid)
                metrics.PRODUCED.labels(run_id=run_id).inc()
            else:
                failed.append({"event_id": eid, "error": str(err)})

        producer.produce(topic, key=ev.order_id, value=payload, on_delivery=_cb)
        producer.poll(0)
        if interval:
            time.sleep(interval)
    producer.flush(120)
    with open(out / "producer_acknowledgements.jsonl", "w", encoding="utf-8") as fh:
        for eid in acked:
            fh.write(json.dumps({"event_id": eid}) + "\n")

    acked_set = set(acked)
    manifest = {
        "run_id": run_id,
        "seed": seed,
        "n_orders": orders,
        "topic": topic,
        "valid_ids": sorted(acked_set & wl.expected_valid_ids),
        "invalid_ids": sorted(acked_set & wl.expected_dlq_ids),
        "attempted": len(wl.stream),
        "acknowledged": len(acked_set),
        "failed_sends": failed,
        "expected_final": wl.expected_final,
    }
    (out / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    # Ground truth for the oracle. NEVER on the investigator's input path.
    truth = Path("results") / "private" / run_id / "ground_truth.json"
    truth.parent.mkdir(parents=True, exist_ok=True)
    truth.write_text(
        json.dumps(
            {
                "run_id": run_id,
                "seed": seed,
                "expected_valid_ids": sorted(wl.expected_valid_ids),
                "expected_dlq_ids": sorted(wl.expected_dlq_ids),
                "expected_final": wl.expected_final,
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    click.echo(f"{run_id}: acked={len(acked_set)} failed={len(failed)}")


if __name__ == "__main__":
    main()  # pylint: disable=no-value-for-parameter
