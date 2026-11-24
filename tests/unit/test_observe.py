"""Unit tests: lag math and evidence-bundle writer (no broker)."""

import json

import pytest

from offsetproof import observe


@pytest.mark.unit
def test_compute_lag_basic():
    lag = observe.compute_lag({0: 100, 1: 50}, {0: 90, 1: 50})
    assert lag == {0: 10, 1: 0}


@pytest.mark.unit
def test_compute_lag_never_negative_and_unknown_stays_unknown():
    lag = observe.compute_lag({0: 100, 1: 5}, {0: 150})
    assert lag[0] == 0  # committed past end (compacted/retention) clamps
    assert lag[1] is None  # no committed offset: unknown, not zero


@pytest.mark.unit
def test_summarize_lag_skips_unknown():
    summary = observe.summarize_lag({0: 10, 1: None, 2: 30})
    assert summary["total_lag"] == 40
    assert summary["max_lag"] == 30
    assert summary["max_lag_partitions"] == [2]
    assert summary["partitions_unknown"] == 1


@pytest.mark.unit
def test_bundle_roundtrip(tmp_path):
    d = observe.write_bundle(
        tmp_path / "run-x",
        manifest={"run_id": "run-x"},
        metric_rows=[{"timestamp_utc": "t", "p0_lag": 3}],
        consumer_logs=[{"level": "info", "event": "committed"}],
        correctness={"valid_missing": []},
    )
    assert (d / "metrics.csv").read_text().splitlines()[0] == "timestamp_utc,p0_lag"
    assert json.loads((d / "correctness_oracle.json").read_text()) == {
        "valid_missing": []
    }
    assert len((d / "consumer_logs.jsonl").read_text().splitlines()) == 1
    assert not (d / "private").exists()
