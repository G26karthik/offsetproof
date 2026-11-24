"""Deterministic rule-based incident diagnosis (Gate C baseline).

Reads ONLY the observer-visible evidence bundle (metrics.csv,
consumer_logs.jsonl). Never reads results/private/ (fault labels, ground
truth). Emits an IncidentReport; says unknown when evidence is
insufficient and ambiguous when signals do not isolate a unique cause.
"""

from __future__ import annotations

import csv
import json
import re
import uuid
from pathlib import Path
from typing import Any

import click

PARTITION_LAG_RE = re.compile(r"^p(\d+)_lag$")

# Share of the latest total lag on a single partition that counts as a
# hot-partition concentration (F4 scenario seeds a visibly skewed key map).
SKEW_SHARE_THRESHOLD = 0.8
# A bad-payload spike needs both a minimum count and a minimum share of
# the log window, so one stray poison record never convicts the batch.
BAD_PAYLOAD_MIN_COUNT = 3
BAD_PAYLOAD_MIN_RATE = 0.10

# Standalone log event names that record a poison/undecodable record.
BAD_EVENT_NAMES = frozenset({"invalid", "undecodable", "none_value"})
# `kind` values on a "committed" record (consumer.py commits the offset
# after a DLQ handoff, so these still count as consumer progress).
BAD_COMMIT_KINDS = frozenset({"invalid", "undecodable"})


def _to_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _read_metrics_rows(path: Path) -> list[dict[str, str]] | None:
    """Return CSV rows, [] when header-only, None when unreadable."""
    try:
        with open(path, encoding="utf-8", newline="") as fh:
            reader = csv.DictReader(fh)
            if reader.fieldnames is None:
                return None
            return [dict(row) for row in reader]
    except (OSError, csv.Error, UnicodeDecodeError):
        return None


def _lag_columns(fieldnames: list[str]) -> list[str]:
    per_partition = [c for c in fieldnames if PARTITION_LAG_RE.match(c)]
    if per_partition:
        return per_partition
    if "lag_total" in fieldnames:
        return ["lag_total"]
    return []


def _read_log_events(path: Path) -> list[dict[str, Any]] | None:
    """Return parsed log records, [] when missing/empty, None if corrupt."""
    if not path.exists():
        return []
    try:
        records: list[dict[str, Any]] = []
        for line in path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                decoded = json.loads(line)
                if isinstance(decoded, dict):
                    records.append(decoded)
        return records
    except (OSError, ValueError, UnicodeDecodeError):
        return None


def _evidence(source: str, query_or_record_id: str, timestamp_utc: str,
              observed_value: str) -> dict[str, str]:
    return {
        "source": source,
        "query_or_record_id": query_or_record_id,
        "timestamp_utc": timestamp_utc,
        "observed_value": observed_value,
    }


def report(
    root_cause_family: str,
    confidence: str,
    symptoms: list[str],
    evidence: list[dict[str, str]],
    counter_evidence: list[str],
    next_checks: list[str],
    suggested_remediation: list[str],
    limitations: list[str],
    time_window: dict[str, str] | None = None,
) -> dict[str, Any]:
    return {
        "incident_id": str(uuid.uuid4()),
        "time_window": time_window or {"start": "", "end": ""},
        "status": "diagnosed" if confidence in ("high", "medium") else (
            "ambiguous" if root_cause_family != "unknown" else "unknown"),
        "root_cause_family": root_cause_family,
        "confidence": confidence,
        "symptoms": symptoms,
        "evidence": evidence,
        "counter_evidence": counter_evidence,
        "next_checks": next_checks,
        "suggested_remediation": suggested_remediation,
        "limitations": limitations,
    }


def _unknown(reason: str, window: dict[str, str]) -> dict[str, Any]:
    return report(
        "unknown", "unknown",
        [reason],
        [],
        ["no measured signal isolates a cause"],
        ["collect a complete metrics.csv + consumer_logs.jsonl window, "
         "then re-run diagnosis"],
        ["human review required"],
        ["evidence bundle incomplete or unreadable; refusing to guess"],
        window,
    )


def diagnose_bundle(bundle_dir: Path) -> dict[str, Any]:
    window: dict[str, str] = {"start": "", "end": ""}
    metrics_path = bundle_dir / "metrics.csv"
    if not metrics_path.exists():
        return _unknown("no metrics.csv in bundle", window)

    rows = _read_metrics_rows(metrics_path)
    if rows is None:
        return _unknown("metrics.csv unreadable", window)
    if not rows:
        return _unknown("metrics.csv has no data rows", window)

    fieldnames = list(rows[0].keys())
    lag_cols = _lag_columns(fieldnames)
    stamps = [str(r.get("timestamp_utc", "")) for r in rows]
    window = {"start": stamps[0], "end": stamps[-1]}

    series: dict[str, list[float | None]] = {
        col: [_to_float(r.get(col)) for r in rows] for col in lag_cols
    }

    def endpoints(values: list[float | None]) -> tuple[float | None, float | None]:
        known = [v for v in values if v is not None]
        if not known:
            return None, None
        return known[0], known[-1]

    ends = {col: endpoints(vals) for col, vals in series.items()}
    usable: dict[str, tuple[float, float]] = {}
    for col, (first, last) in ends.items():
        if first is not None and last is not None:
            usable[col] = (first, last)

    def row_total(idx: int) -> float | None:
        vals: list[float] = []
        for c in series:
            cell = series[c][idx]
            if cell is not None:
                vals.append(cell)
        return float(sum(vals)) if vals else None

    totals = [row_total(i) for i in range(len(rows))]
    known_totals = [t for t in totals if t is not None]
    first_total = known_totals[0] if known_totals else None
    last_total = known_totals[-1] if known_totals else None
    latest_vals: dict[str, float] = {}
    for c in series:
        last_val = series[c][-1]
        if last_val is not None:
            latest_vals[c] = last_val

    logs = _read_log_events(bundle_dir / "consumer_logs.jsonl")
    if logs is None:
        return _unknown("consumer_logs.jsonl is corrupt", window)
    log_count = len(logs)
    committed = sum(1 for e in logs if e.get("event") == "committed")
    bad = sum(
        1 for e in logs
        if e.get("event") in BAD_EVENT_NAMES
        or (e.get("event") == "committed" and e.get("kind") in BAD_COMMIT_KINDS)
    )
    kafka_errors = sum(1 for e in logs if e.get("event") == "kafka_error")
    bad_rate = (bad / log_count) if log_count else 0.0

    base_limitations = [
        "observer-visible evidence only; fault labels and ground truth "
        "never consulted",
    ]

    def lag_evidence() -> list[dict[str, str]]:
        items = []
        for col in lag_cols:
            first, last = ends[col]
            if first is not None and last is not None:
                items.append(_evidence(
                    "metrics.csv", f"{col} first->last", window["end"],
                    f"{first:g} -> {last:g}"))
        return items

    # No lag signal and no log signal: nothing to reason from.
    if not usable and log_count == 0:
        return _unknown("no per-partition lag series and no log events", window)

    lag_climbs_everywhere = (
        bool(usable)
        and first_total is not None and last_total is not None
        and last_total > first_total
        and all(last >= first for first, last in usable.values())
        and any(last > first for first, last in usable.values())
    )
    skew_share = 0.0
    skew_col = ""
    if last_total is not None and last_total > 0 and len(latest_vals) >= 2:
        skew_col = max(latest_vals, key=lambda c: latest_vals[c])
        skew_share = latest_vals[skew_col] / last_total
    skewed = skew_share >= SKEW_SHARE_THRESHOLD
    bad_spike = (bad >= BAD_PAYLOAD_MIN_COUNT
                 and bad_rate >= BAD_PAYLOAD_MIN_RATE)
    sink_not_draining = (
        committed > 0 and first_total is not None
        and last_total is not None
        and last_total >= first_total and last_total > 0
    )

    # Priority: no-progress beats concentration beats poison beats
    # not-draining, because slow_sink's condition (commits advance, lag
    # does not drain) also holds during skew and poison incidents.
    if committed == 0 and lag_climbs_everywhere:
        first_s, last_s = first_total, last_total
        assert first_s is not None and last_s is not None
        return report(
            "stalled_consumer", "high",
            [f"total lag rose {first_s:g} -> {last_s:g} across "
             f"{len(usable)} partition(s) with zero committed events "
             f"over {len(rows)} samples"],
            lag_evidence() + [_evidence(
                "consumer_logs.jsonl", "count event=committed",
                window["end"], f"{committed} committed of {log_count} records")],
            [f"kafka_error events: {kafka_errors} (broker disruption is "
             "not a ruled-in cause; see next checks)"],
            ["inspect consumer group membership and rebalance events",
             "check consumer process/container liveness at window end",
             "rule out broker unreachability via kafka_admin ISR/leader state"],
            ["human review: restart stalled consumer(s) after confirming "
             "group membership, then watch lag drain"],
            base_limitations + [
                "cannot distinguish crashed consumer from wedged poll "
                "loop on lag alone"],
            window,
        )

    if skewed:
        assert last_total is not None
        return report(
            "partition_skew", "high",
            [f"{skew_col} holds {skew_share:.0%} of total lag "
             f"({latest_vals[skew_col]:g} of {last_total:g})"],
            lag_evidence() + [_evidence(
                "metrics.csv", f"{skew_col} share of latest total",
                window["end"], f"{skew_share:.3f}")],
            ["consumer commits observed" if committed > 0 else
             "no consumer commits in window",
             f"bad-payload records: {bad} (poison input not the driver)"],
            ["verify key-to-partition routing for the hot partition",
             "check per-partition produce rate vs consume rate",
             "review partition assignment across group members"],
            ["human review: re-key or re-partition the hot key range; "
             "adding idle consumers alone will not drain one hot partition"],
            base_limitations + [
                "routing skew vs single-partition slowness needs "
                "per-partition timing, not just lag"],
            window,
        )

    if bad_spike:
        return report(
            "bad_payload", "high" if bad_rate >= 0.20 else "medium",
            [f"{bad} of {log_count} log records are invalid/undecodable "
             f"({bad_rate:.0%})"],
            [_evidence(
                "consumer_logs.jsonl", "count invalid|undecodable|bad-kind",
                window["end"], f"{bad} of {log_count} records")]
            + lag_evidence(),
            ["lag concentration below skew threshold"
             if not skewed else "lag also concentrated; poison treated as "
             "primary because DLQ handoff explains continued commits"],
            ["inspect DLQ records and error classifications",
             "identify the producing batch/schema change behind the spike",
             "confirm valid events after the poison batch still commit"],
            ["human review: fix the producing payload or schema, "
             "quarantine the poison batch; do not reset offsets past "
             "unprocessed valid events"],
            base_limitations + [
                "log-rate spike localizes the batch, not the code defect"],
            window,
        )

    if sink_not_draining:
        first_s, last_s = first_total, last_total
        assert first_s is not None and last_s is not None
        return report(
            "slow_sink", "medium",
            [f"{committed} commits advanced but total lag did not drain "
             f"({first_s:g} -> {last_s:g})"],
            lag_evidence() + [_evidence(
                "consumer_logs.jsonl", "count event=committed",
                window["end"], f"{committed} committed of {log_count} records")],
            ["per-partition lag concentration below skew threshold",
             f"bad-payload records: {bad} (low; poison not the driver)",
             "a producer burst can mimic a slow sink on lag alone"],
            ["compare DB write-latency histogram against broker produce "
             "latency in the same window",
             "check consumer processing-duration percentiles",
             "confirm produce rate did not simply exceed capacity (F0 burst)"],
            ["human review: relieve the sink (tune DB writes, scale "
             "sink capacity) after latency evidence confirms it"],
            base_limitations + [
                "lag-plus-commits cannot separate a slow sink from a "
                "producer burst; latency evidence required"],
            window,
        )

    return report(
        "ambiguous" if (usable or log_count) else "unknown",
        "low" if (usable or log_count) else "unknown",
        ["signals present but no rule isolates a unique cause"
         if (usable or log_count) else "no usable signals"],
        lag_evidence(),
        ["no second corroborating signal for any single cause family"],
        ["widen the window and re-run",
         "add per-partition timing and DB latency evidence"],
        ["human review required"],
        base_limitations + ["refusing to claim a cause from a single "
                            "generic symptom"],
        window,
    )


@click.command()
@click.option("--mode", type=click.Choice(["rules", "agent"]), default="rules")
@click.option("--run", "run_dir", required=True)
def main(mode: str, run_dir: str) -> None:
    bundle = Path(run_dir)
    if mode == "agent":
        from .agent.pipeline import investigate

        incident = investigate(bundle)
        (bundle / "incident_agent.json").write_text(
            json.dumps(incident, indent=2), encoding="utf-8")
        click.echo(json.dumps(incident, indent=2))
        return
    incident = diagnose_bundle(bundle)
    (bundle / "incident_rule.json").write_text(json.dumps(incident, indent=2),
                                               encoding="utf-8")
    click.echo(json.dumps(incident, indent=2))


if __name__ == "__main__":
    main()  # pylint: disable=no-value-for-parameter
