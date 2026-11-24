"""Unit tests: deterministic Gate C rule diagnosis (no broker, no DB)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from offsetproof import diagnose, observe


def _rows(spec: dict[str, list[float]], n: int | None = None) -> list[dict[str, Any]]:
    count = n or len(next(iter(spec.values())))
    rows: list[dict[str, Any]] = []
    for i in range(count):
        row: dict[str, Any] = {"timestamp_utc": f"2026-11-24T00:00:{i:02d}Z"}
        for col, vals in spec.items():
            row[col] = vals[i]
        rows.append(row)
    return rows


def _bundle(tmp_path: Path, name: str, metric_rows: list[dict[str, Any]],
            logs: list[dict[str, Any]]) -> Path:
    return observe.write_bundle(
        tmp_path / name,
        manifest={"run_id": name},
        metric_rows=metric_rows,
        consumer_logs=logs,
    )


def _committed(partition: int, offset: int, kind: str = "valid") -> dict[str, Any]:
    return {"level": "info", "event": "committed", "partition": partition,
            "offset": offset, "kind": kind}


@pytest.mark.unit
def test_stalled_consumer(tmp_path: Path):
    d = _bundle(tmp_path, "stalled",
                _rows({"p0_lag": [10, 20, 35, 50], "p1_lag": [5, 12, 20, 30]}),
                [{"level": "info", "event": "assigned", "partitions": []}])
    incident = diagnose.diagnose_bundle(d)
    assert incident["root_cause_family"] == "stalled_consumer"
    assert incident["status"] == "diagnosed"
    assert incident["confidence"] == "high"
    assert len(incident["evidence"]) >= 2  # lag series + zero-commit count
    assert incident["time_window"]["start"].endswith("00Z")
    assert incident["time_window"]["end"].endswith("03Z")


@pytest.mark.unit
def test_slow_sink(tmp_path: Path):
    logs = [_committed(0, i) for i in range(10)]
    d = _bundle(tmp_path, "slow",
                _rows({"p0_lag": [100, 100, 101, 100],
                       "p1_lag": [90, 91, 90, 90]}),
                logs)
    incident = diagnose.diagnose_bundle(d)
    assert incident["root_cause_family"] == "slow_sink"
    assert incident["status"] == "diagnosed"
    assert incident["confidence"] == "medium"


@pytest.mark.unit
def test_partition_skew(tmp_path: Path):
    logs = [_committed(0, i) for i in range(6)]
    d = _bundle(tmp_path, "skew",
                _rows({"p0_lag": [10, 40, 70, 90], "p1_lag": [10, 10, 10, 10]}),
                logs)
    incident = diagnose.diagnose_bundle(d)
    assert incident["root_cause_family"] == "partition_skew"
    assert incident["status"] == "diagnosed"
    assert incident["confidence"] == "high"


@pytest.mark.unit
def test_bad_payload(tmp_path: Path):
    logs = [_committed(0, i) for i in range(6)]
    logs += [{"level": "warn", "event": "invalid", "detail": "unknown_event_type"}
             for _ in range(3)]
    logs += [{"level": "warn", "event": "undecodable"} for _ in range(2)]
    d = _bundle(tmp_path, "poison",
                _rows({"p0_lag": [50, 35, 20, 10], "p1_lag": [40, 30, 20, 10]}),
                logs)
    incident = diagnose.diagnose_bundle(d)
    assert incident["root_cause_family"] == "bad_payload"
    assert incident["status"] == "diagnosed"


@pytest.mark.unit
def test_bad_payload_via_commit_kind(tmp_path: Path):
    logs = [_committed(0, i, kind="invalid") for i in range(5)]
    logs += [_committed(1, i) for i in range(5)]
    d = _bundle(tmp_path, "poison-kind",
                _rows({"p0_lag": [30, 20, 12, 8], "p1_lag": [30, 22, 14, 8]}),
                logs)
    incident = diagnose.diagnose_bundle(d)
    assert incident["root_cause_family"] == "bad_payload"


@pytest.mark.unit
def test_lag_total_column_supported(tmp_path: Path):
    d = _bundle(tmp_path, "total",
                _rows({"lag_total": [20, 40, 60, 80]}),
                [{"level": "info", "event": "assigned", "partitions": []}])
    incident = diagnose.diagnose_bundle(d)
    assert incident["root_cause_family"] == "stalled_consumer"


@pytest.mark.unit
def test_ambiguous_healthy_window(tmp_path: Path):
    logs = [_committed(0, i) for i in range(4)]
    d = _bundle(tmp_path, "healthy",
                _rows({"p0_lag": [0, 0, 0, 0], "p1_lag": [0, 0, 0, 0]}),
                logs)
    incident = diagnose.diagnose_bundle(d)
    assert incident["root_cause_family"] == "ambiguous"
    assert incident["status"] == "ambiguous"
    assert incident["confidence"] == "low"


@pytest.mark.unit
def test_unknown_missing_bundle(tmp_path: Path):
    incident = diagnose.diagnose_bundle(tmp_path / "does-not-exist")
    assert incident["root_cause_family"] == "unknown"
    assert incident["status"] == "unknown"


@pytest.mark.unit
def test_unknown_header_only_metrics(tmp_path: Path):
    d = tmp_path / "empty"
    d.mkdir()
    (d / "metrics.csv").write_text("timestamp_utc,p0_lag\n", encoding="utf-8")
    incident = diagnose.diagnose_bundle(d)
    assert incident["root_cause_family"] == "unknown"
    assert incident["status"] == "unknown"
