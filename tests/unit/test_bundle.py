"""Unit tests: bundle capture and benchmark planning (no broker)."""

import pytest
from click.testing import CliRunner

from offsetproof.eval import benchmark
from offsetproof.ops import bundle


@pytest.mark.unit
def test_append_snapshot_creates_and_appends(tmp_path):
    run = tmp_path / "run-x"
    snap = {"timestamp_utc": "t1", "lag_total": 5, "p0_lag": 5}
    bundle.append_snapshot(run, snap)
    bundle.append_snapshot(run, {**snap, "timestamp_utc": "t2"})
    text = (run / "metrics.csv").read_text(encoding="utf-8")
    assert text.splitlines()[0] == "timestamp_utc,lag_total,p0_lag"
    assert len(text.splitlines()) == 3


@pytest.mark.unit
def test_benchmark_dry_run_plans_without_executing(tmp_path):
    plan = tmp_path / "plan.yaml"
    plan.write_text("seed: 1\nfault_trials: 2\n", encoding="utf-8")
    result = CliRunner().invoke(
        benchmark.main,
        ["--config", str(plan), "--out", str(tmp_path / "out"), "--dry-run"],
    )
    assert result.exit_code == 0
    assert (tmp_path / "out").exists()
