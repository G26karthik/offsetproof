"""Unit tests: investigator validation with stub adapter (no network)."""

import json

import pytest

from offsetproof.agent import pipeline, tools
from offsetproof.agent.llm import StubAdapter


def _bundle(tmp_path) -> object:
    d = tmp_path / "bundle"
    d.mkdir()
    (d / "metrics.csv").write_text(
        "timestamp_utc,p0_lag,p1_lag\nt1,10,10\nt2,50,50\n", encoding="utf-8")
    (d / "consumer_logs.jsonl").write_text(
        '{"event": "committed", "partition": 0}\n', encoding="utf-8")
    (d / "runbook.md").write_text("lag means backlog\n", encoding="utf-8")
    return d


def _good_reply() -> str:
    return json.dumps({
        "time_window": {"start": "t1", "end": "t2"},
        "status": "diagnosed",
        "root_cause_family": "slow_sink",
        "confidence": "medium",
        "symptoms": ["lag rose with commits advancing"],
        "evidence": [{"source": "metrics.csv", "query_or_record_id": "p0_lag",
                      "timestamp_utc": "t2", "observed_value": "50"}],
        "counter_evidence": ["burst possible"],
        "next_checks": ["latency histograms"],
        "suggested_remediation": ["human review"],
        "limitations": ["no latency evidence"],
    })


@pytest.mark.unit
def test_valid_reply_passes_with_citations(tmp_path):
    out = pipeline.investigate(_bundle(tmp_path), StubAdapter([_good_reply()]))
    assert out["root_cause_family"] == "slow_sink"
    assert out["status"] == "diagnosed"
    assert out["evidence"][0]["source"] == "metrics.csv"
    assert out["limitations"][-1].startswith("machine-proposed")


@pytest.mark.unit
def test_bad_json_abstains(tmp_path):
    out = pipeline.investigate(_bundle(tmp_path), StubAdapter(["not json"]))
    assert out["status"] == "unknown"


@pytest.mark.unit
def test_unsupported_evidence_source_rejected(tmp_path):
    bad = json.loads(_good_reply())
    bad["evidence"] = [{"source": "secret_labels.csv",
                        "timestamp_utc": "t", "observed_value": "x"}]
    out = pipeline.investigate(
        _bundle(tmp_path), StubAdapter([json.dumps(bad)]))
    assert out["status"] == "unknown"


@pytest.mark.unit
def test_empty_reply_abstains(tmp_path):
    out = pipeline.investigate(_bundle(tmp_path), StubAdapter([]))
    assert out["status"] == "unknown"


@pytest.mark.unit
def test_provider_parsing():
    from offsetproof.agent.llm import HttpAdapter

    openai_payload = {"choices": [{"message": {"content": "hi"}}]}
    assert HttpAdapter.parse_openai(openai_payload) == "hi"
    anthropic_payload = {"content": [{"type": "text", "text": "a"},
                                     {"type": "text", "text": "b"}]}
    assert HttpAdapter.parse_anthropic(anthropic_payload) == "ab"
    with pytest.raises(ValueError):
        HttpAdapter.parse_anthropic({"content": []})


@pytest.mark.unit
def test_tools_reject_escape_and_non_allowlisted(tmp_path):
    d = _bundle(tmp_path)
    with pytest.raises(ValueError):
        tools.read_evidence(d, "manifest.json/../x")
    with pytest.raises(ValueError):
        tools.read_evidence(d, "ground_truth.json")
    assert "lag" in tools.read_evidence(d, "runbook.md")
