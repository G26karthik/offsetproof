"""Unit tests: study scorer handles missing agent reports."""

import json

import pytest
from click.testing import CliRunner

from offsetproof.eval import score_study


@pytest.mark.unit
def test_missing_agent_counts_as_abstention(tmp_path):
    bundle = tmp_path / "b"
    bundle.mkdir()
    (bundle / "incident_rule.json").write_text(json.dumps({
        "root_cause_family": "bad_payload", "status": "diagnosed",
        "evidence": [{"source": "m", "timestamp_utc": "t",
                      "observed_value": "1"}],
    }), encoding="utf-8")
    labels = tmp_path / "labels.json"
    labels.write_text(json.dumps({str(bundle): "bad_payload"}),
                      encoding="utf-8")
    out = tmp_path / "summary.json"
    result = CliRunner().invoke(
        score_study.main, ["--labels", str(labels), "--out", str(out)])
    assert result.exit_code == 0
    summary = json.loads(out.read_text(encoding="utf-8"))
    assert summary["agent_top1"] == 0.0
    assert summary["agent_unknown_rate"] == 1.0
    assert summary["rule_top1"] == 1.0


@pytest.mark.unit
def test_missing_rule_report_refuses(tmp_path):
    labels = tmp_path / "labels.json"
    labels.write_text(json.dumps({str(tmp_path / "empty"): "x"}),
                      encoding="utf-8")
    result = CliRunner().invoke(score_study.main,
                                ["--labels", str(labels)])
    assert result.exit_code != 0
