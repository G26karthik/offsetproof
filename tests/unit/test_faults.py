"""Unit tests: fault runner guards + blinded comparison scoring."""

import pytest
from click.testing import CliRunner

from offsetproof.eval import compare
from offsetproof.faults import _guard_container
from offsetproof.faults import run as fault_run


@pytest.mark.unit
def test_guard_rejects_foreign_container():
    with pytest.raises(ValueError):
        _guard_container("postgres")
    assert _guard_container("offsetproof-kafka-1") == "offsetproof-kafka-1"


@pytest.mark.unit
def test_dry_run_prints_without_executing(tmp_path):
    result = CliRunner().invoke(
        fault_run.main,
        ["--scenario", "consumer-crash", "--run-dir", str(tmp_path / "r")],
    )
    assert result.exit_code == 0
    assert "docker kill" in result.output
    assert (tmp_path / "r").exists()


@pytest.mark.unit
def test_destructive_requires_opt_in(tmp_path):
    result = CliRunner().invoke(
        fault_run.main,
        ["--scenario", "broker-restart", "--run-dir", str(tmp_path / "r"),
         "--execute"],
    )
    assert result.exit_code != 0
    assert "i-understand" in result.output


@pytest.mark.unit
def test_evidence_precision_counts_supported_claims():
    incident = {"evidence": [
        {"source": "m", "timestamp_utc": "t", "observed_value": "1"},
        {"source": "", "timestamp_utc": "t", "observed_value": "2"},
    ]}
    assert compare.evidence_precision(incident) == 0.5
    assert compare.evidence_precision({"evidence": []}) is None


@pytest.mark.unit
def test_summarize_counts_never_vibes():
    scores = [
        compare.score_run(
            "stalled_consumer",
            {"root_cause_family": "stalled_consumer", "status": "diagnosed",
             "evidence": [{"source": "m", "timestamp_utc": "t",
                           "observed_value": "1"}]},
            {"root_cause_family": "unknown", "status": "unknown",
             "evidence": []},
        ),
        compare.score_run(
            "bad_payload",
            {"root_cause_family": "unknown", "status": "unknown",
             "evidence": []},
            {"root_cause_family": "bad_payload", "status": "diagnosed",
             "evidence": [{"source": "l", "timestamp_utc": "t",
                           "observed_value": "9"}]},
        ),
    ]
    s = compare.summarize(scores)
    assert s == {
        "runs": 2,
        "rule_top1": 0.5,
        "agent_top1": 0.5,
        "rule_unknown_rate": 0.5,
        "agent_unknown_rate": 0.5,
        "rule_precision": 1.0,
        "agent_precision": 1.0,
    }
