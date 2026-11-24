"""Blinded rule-vs-agent comparison (Gate D harness). Pure scoring.

Takes incident pairs from the SAME frozen runs and scores: top-1 family
agreement with the held-out label, unknown rates, evidence-support
precision (fraction of evidence entries carrying non-empty source,
timestamp, and observed value — unsupported claims fail, never silently
dropped). No thresholds hidden: everything is returned raw.
"""

from __future__ import annotations

from typing import Any


def evidence_precision(incident: dict[str, Any]) -> float | None:
    """Fraction of evidence entries with source+timestamp+value.

    Returns None when the incident cites no evidence at all (scored
    separately from zero-precision, which means it cited junk).
    """
    entries = incident.get("evidence") or []
    if not entries:
        return None
    good = sum(
        1 for e in entries
        if isinstance(e, dict) and e.get("source") and e.get("timestamp_utc")
        and e.get("observed_value")
    )
    return good / len(entries)


def score_run(
    label: str, rule: dict[str, Any], agent: dict[str, Any] | None
) -> dict[str, Any]:
    """Score one blinded run. `agent` may be None (agent abstained/unrun)."""
    rule_hit = rule.get("root_cause_family") == label
    out: dict[str, Any] = {
        "label": label,
        "rule_family": rule.get("root_cause_family"),
        "rule_hit": rule_hit,
        "rule_unknown": rule.get("status") == "unknown",
        "rule_precision": evidence_precision(rule),
        "agent_family": (agent or {}).get("root_cause_family"),
        "agent_hit": (agent or {}).get("root_cause_family") == label,
        "agent_unknown": (agent or {}).get("status") in ("unknown", None),
        "agent_precision": evidence_precision(agent) if agent else None,
    }
    return out


def summarize(scores: list[dict[str, Any]]) -> dict[str, Any]:
    """Aggregate a frozen evaluation set. Counts, never vibes."""
    n = len(scores)
    if not n:
        return {"runs": 0}

    def mean(vals: list[float | None]) -> float | None:
        known = [v for v in vals if v is not None]
        return sum(known) / len(known) if known else None

    return {
        "runs": n,
        "rule_top1": sum(1 for s in scores if s["rule_hit"]) / n,
        "agent_top1": sum(1 for s in scores if s["agent_hit"]) / n,
        "rule_unknown_rate": sum(1 for s in scores if s["rule_unknown"]) / n,
        "agent_unknown_rate": sum(1 for s in scores if s["agent_unknown"]) / n,
        "rule_precision": mean([s["rule_precision"] for s in scores]),
        "agent_precision": mean([s["agent_precision"] for s in scores]),
    }
