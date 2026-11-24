"""Evidence-grounded investigation pipeline. Model advises; code decides.

The model proposes an IncidentReport as JSON. This module validates the
structure, checks every cited fact against the bundle it was given, and
falls back to `unknown` on any failure. A model that cannot prove its
claims contributes nothing — by design.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ..diagnose import report as make_report
from . import tools
from .llm import HttpAdapter, StubAdapter

FAMILIES = frozenset(
    {"stalled_consumer", "slow_sink", "partition_skew", "bad_payload",
     "broker_disruption", "unknown"}
)
CONFIDENCES = frozenset({"high", "medium", "low", "unknown"})


def _unknown(reason: str) -> dict[str, Any]:
    return make_report(
        "unknown", "unknown", [reason], [],
        ["model output unsupported or adapter unavailable"],
        ["collect a complete evidence bundle and re-run"],
        ["human review required"],
        ["investigator abstained rather than guess"],
    )


def _validate(data: Any) -> dict[str, Any] | None:
    if not isinstance(data, dict):
        return None
    if data.get("root_cause_family") not in FAMILIES:
        return None
    if data.get("confidence") not in CONFIDENCES:
        return None
    for key in ("symptoms", "evidence", "limitations"):
        if not isinstance(data.get(key), list):
            return None
    for entry in data["evidence"]:
        if not isinstance(entry, dict):
            return None
        for field in ("source", "timestamp_utc", "observed_value"):
            if not entry.get(field):
                return None
        if entry.get("source") not in (
            "metrics.csv", "consumer_logs.jsonl", "runbook.md"
        ):
            return None
    return data


def build_prompt(bundle_dir: Path) -> str:
    metrics = tools.read_evidence(bundle_dir, "metrics.csv")
    logs = tools.read_evidence(bundle_dir, "consumer_logs.jsonl")
    runbook_hits = tools.search_runbook(
        bundle_dir, ["lag", "skew", "poison", "rebalance"])
    return (
        "You are a read-only Kafka incident analyst. Use ONLY the evidence "
        "below. Reply with a single JSON object with keys incident_id (omit), "
        "time_window {start,end}, status, root_cause_family "
        "(stalled_consumer|slow_sink|partition_skew|bad_payload|"
        "broker_disruption|unknown), confidence (high|medium|low|unknown), "
        "symptoms[], evidence[] ({source, query_or_record_id, timestamp_utc, "
        "observed_value}), counter_evidence[], next_checks[], "
        "suggested_remediation[], limitations[]. Every evidence entry must "
        "quote a record actually present below; if none supports a cause, "
        "say unknown.\n\n## metrics.csv\n" + metrics
        + "\n\n## consumer_logs.jsonl\n" + logs
        + "\n\n## runbook\n" + "\n".join(runbook_hits)
    )


def investigate(
    bundle_dir: Path, adapter: StubAdapter | HttpAdapter | None = None
) -> dict[str, Any]:
    adapter = adapter or HttpAdapter()
    try:
        prompt = build_prompt(bundle_dir)
    except (ValueError, OSError) as exc:
        return _unknown(f"evidence unreadable: {exc}")
    try:
        text = adapter.complete(prompt)
    except Exception:  # noqa: BLE001 - adapter failure abstains
        return _unknown("adapter raised; abstaining")
    if not text:
        return _unknown("adapter unavailable or empty reply")
    try:
        data = json.loads(text[text.index("{"): text.rindex("}") + 1])
    except (ValueError, IndexError):
        return _unknown("adapter reply was not JSON")
    valid = _validate(data)
    if valid is None:
        return _unknown("adapter reply failed schema/citation validation")
    return make_report(
        str(valid["root_cause_family"]),
        str(valid["confidence"]),
        [str(s) for s in valid.get("symptoms", [])],
        [
            {"source": str(e.get("source", "")),
             "query_or_record_id": str(e.get("query_or_record_id", "")),
             "timestamp_utc": str(e.get("timestamp_utc", "")),
             "observed_value": str(e.get("observed_value", ""))}
            for e in valid.get("evidence", [])
        ],
        [str(c) for c in valid.get("counter_evidence", [])],
        [str(n) for n in valid.get("next_checks", [])],
        [str(r) for r in valid.get("suggested_remediation", [])],
        [str(lim) for lim in valid.get("limitations", [])]
        + ["machine-proposed diagnosis; human must confirm before acting"],
        ({"start": str(valid.get("time_window", {}).get("start", "")),
          "end": str(valid.get("time_window", {}).get("end", ""))}
         if isinstance(valid.get("time_window"), dict) else None),
    )
