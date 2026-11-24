"""Observer-side telemetry helpers (Gate B). All pure logic first.

Every function takes plain data and returns plain data, so the math is
unit-testable without a broker. Thin collectors that call the Admin API
live at the bottom and are integration-tested against the real stack.
"""

from __future__ import annotations

import csv
import io
import json
from pathlib import Path
from typing import Any


def compute_lag(
    end_offsets: dict[int, int], committed: dict[int, int]
) -> dict[int, int | None]:
    """Per-partition group lag = log-end minus committed group offset.

    A partition with no committed offset (new/unconsumed group) has unknown
    lag, reported as None — never silently zero.
    """
    lag: dict[int, int | None] = {}
    for partition, end in end_offsets.items():
        base = committed.get(partition)
        lag[partition] = max(end - base, 0) if base is not None else None
    return lag


def summarize_lag(lag: dict[int, int | None]) -> dict[str, Any]:
    """Total/max lag over partitions with known offsets."""
    known = {p: v for p, v in lag.items() if v is not None}
    return {
        "partitions_known": len(known),
        "partitions_unknown": len(lag) - len(known),
        "total_lag": sum(known.values()),
        "max_lag": max(known.values()) if known else 0,
        "max_lag_partitions": sorted(p for p, v in known.items()
                                     if v == max(known.values())) if known else [],
    }


def render_metrics_csv(rows: list[dict[str, Any]]) -> str:
    """Render sampled signals to CSV with a stable column order."""
    if not rows:
        return "timestamp_utc\n"
    columns = ["timestamp_utc"] + sorted(k for k in rows[0] if k != "timestamp_utc")
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=columns, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(rows)
    return buf.getvalue()


def write_bundle(
    bundle_dir: Path,
    manifest: dict[str, Any],
    metric_rows: list[dict[str, Any]],
    consumer_logs: list[dict[str, Any]],
    correctness: dict[str, Any] | None = None,
    incident_rule: dict[str, Any] | None = None,
    incident_agent: dict[str, Any] | None = None,
) -> Path:
    """Write the observer-visible evidence bundle. No ground truth in here."""
    bundle_dir.mkdir(parents=True, exist_ok=True)
    (bundle_dir / "manifest.json").write_text(json.dumps(manifest, indent=2),
                                              encoding="utf-8")
    (bundle_dir / "metrics.csv").write_text(render_metrics_csv(metric_rows),
                                            encoding="utf-8")
    with open(bundle_dir / "consumer_logs.jsonl", "w", encoding="utf-8") as fh:
        for line in consumer_logs:
            fh.write(json.dumps(line) + "\n")
    if correctness is not None:
        (bundle_dir / "correctness_oracle.json").write_text(
            json.dumps(correctness, indent=2), encoding="utf-8")
    if incident_rule is not None:
        (bundle_dir / "incident_rule.json").write_text(
            json.dumps(incident_rule, indent=2), encoding="utf-8")
    if incident_agent is not None:
        (bundle_dir / "incident_agent.json").write_text(
            json.dumps(incident_agent, indent=2), encoding="utf-8")
    return bundle_dir
