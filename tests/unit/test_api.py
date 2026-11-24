"""Unit tests: API helpers over fixture bundles (no HTTP client, no DB)."""

import json

import pytest

from offsetproof import api


def _run(tmp_path, name="run-abc", manifest=None):
    d = tmp_path / name
    d.mkdir(parents=True, exist_ok=True)
    m = {"run_id": name, "profile": "orders", "seed": 1,
         "attempted": 10, "acknowledged": 10}
    m.update(manifest or {})
    (d / "manifest.json").write_text(json.dumps(m), encoding="utf-8")
    return d


@pytest.mark.unit
def test_list_runs_empty_and_present(tmp_path):
    assert api.list_runs(tmp_path / "nope") == []
    _run(tmp_path)
    (tmp_path / "junk.txt").write_text("x", encoding="utf-8")
    runs = api.list_runs(tmp_path)
    assert [r["run_id"] for r in runs] == ["run-abc"]
    assert runs[0]["profile"] == "orders"


@pytest.mark.unit
def test_load_run_bundle_404_and_bad_id(tmp_path):
    body, status = api.load_run_bundle("run-abc", tmp_path)
    assert status == 404
    body, status = api.load_run_bundle("../x", tmp_path)
    assert status == 400
    d = _run(tmp_path)
    (d / "incident_rule.json").write_text(
        json.dumps({"root_cause_family": "slow_sink"}), encoding="utf-8")
    body, status = api.load_run_bundle("run-abc", tmp_path)
    assert status == 200
    assert body["incident_rule"]["root_cause_family"] == "slow_sink"


@pytest.mark.unit
def test_read_metrics_unavailable_and_capped(tmp_path):
    d = _run(tmp_path)
    body, status = api.read_metrics("run-abc", results_dir=tmp_path)
    assert status == 200 and body["unavailable"] is True
    (d / "metrics.csv").write_text(
        "timestamp_utc,p0_lag\n" + "".join(f"t{i},{i}\n" for i in range(10)),
        encoding="utf-8")
    body, status = api.read_metrics("run-abc", limit=3, results_dir=tmp_path)
    assert status == 200 and len(body["rows"]) == 3
    assert body["rows"][0]["p0_lag"] == "0"
