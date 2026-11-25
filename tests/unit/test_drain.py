"""Unit tests: bundle log splitting attributes by event id."""

import json

import pytest

from offsetproof.ops import drain


def _line(event, **kw):
    d = {"event": event}
    d.update(kw)
    return json.dumps(d) + "\n"


@pytest.mark.unit
def test_split_logs_attributes_only_matching_commits(tmp_path):
    shared = tmp_path / "shared.log"
    shared.write_text(
        "".join([
            _line("started"),
            _line("assigned"),
            _line("committed", event_id="AAA", kind="valid"),
            _line("committed", event_id=None, kind="invalid"),
            _line("committed", event_id="BBB", kind="invalid"),
            _line("kafka_error", error="x"),
            "not json\n",
        ]),
        encoding="utf-8",
    )
    run = tmp_path / "run"
    run.mkdir()
    manifest = {"valid_ids": ["AAA"], "invalid_ids": ["BBB"]}
    drain.split_logs(shared, run, manifest)
    lines = (run / "consumer_logs.jsonl").read_text(encoding="utf-8")
    assert "AAA" in lines and "BBB" in lines
    assert "started" in lines and "kafka_error" in lines
    assert "not json" not in lines
    # The null-id commit from another run's poison must not leak in.
    assert lines.count("committed") == 2


@pytest.mark.unit
def test_split_logs_other_run_ids_excluded(tmp_path):
    shared = tmp_path / "shared.log"
    shared.write_text(
        _line("committed", event_id="ZZZ-other-run", kind="valid"),
        encoding="utf-8",
    )
    run = tmp_path / "run"
    run.mkdir()
    drain.split_logs(shared, run, {"valid_ids": ["AAA"], "invalid_ids": []})
    assert (run / "consumer_logs.jsonl").read_text(encoding="utf-8") == ""
