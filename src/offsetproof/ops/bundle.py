"""Capture an observer-visible evidence bundle for a run (Gate B).

Appends one lag snapshot row to metrics.csv and writes manifest.json.
Logs are the consumer's own stdout (redirect it to consumer_logs.jsonl
when launching the consumer). Never touches results/private/.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Callable

import click

from offsetproof.collect import collect_live
from offsetproof.observe import render_metrics_csv


def append_snapshot(
    run_dir: Path,
    snapshot: dict[str, Any],
    collect: Callable[[], dict[str, Any]] | None = None,
) -> Path:
    """Append one snapshot row to metrics.csv (injectable for tests)."""
    run_dir.mkdir(parents=True, exist_ok=True)
    row = (collect or (lambda: snapshot))()
    metrics_path = run_dir / "metrics.csv"
    existing: list[dict[str, Any]] = []
    if metrics_path.exists():
        with open(metrics_path, encoding="utf-8", newline="") as fh:
            existing = [dict(r) for r in csv.DictReader(fh)]
    existing.append(row)
    # newline="": the csv renderer already emits \r\n; text-mode translation
    # would double every line break on Windows and corrupt row counts.
    with open(metrics_path, "w", encoding="utf-8", newline="") as fh:
        fh.write(render_metrics_csv(existing))
    return metrics_path


@click.command()
@click.option("--run-dir", required=True)
@click.option("--bootstrap", default="localhost:29092")
@click.option("--topic", default="orders.events")
@click.option("--group-id", default="offsetproof-projector")
def main(run_dir: str, bootstrap: str, topic: str, group_id: str) -> None:
    snap = collect_live(bootstrap, topic, group_id)
    path = append_snapshot(Path(run_dir), snap)
    manifest_path = Path(run_dir) / "manifest.json"
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        manifest = {}
    # Merge telemetry context; never drop the producer's ack manifest,
    # which the oracle needs (valid_ids, expected_final, dataset).
    manifest.update({"run_dir": run_dir, "topic": topic,
                     "group_id": group_id, "latest": snap})
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    click.echo(f"snapshot appended to {path}")


if __name__ == "__main__":
    main()  # pylint: disable=no-value-for-parameter
