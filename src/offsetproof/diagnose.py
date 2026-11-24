"""Deterministic rule-based incident diagnosis (Gate C baseline).

Reads ONLY the observer-visible evidence bundle (metrics.csv, logs, admin
snapshots). Never reads results/private/ (fault labels, ground truth).
Emits an IncidentReport; says unknown when evidence is insufficient.
"""

from __future__ import annotations

import json
import uuid
from pathlib import Path
from typing import Any

import click


def diagnose_bundle(bundle_dir: Path) -> dict[str, Any]:
    metrics_path = bundle_dir / "metrics.csv"
    if not metrics_path.exists():
        return report("unknown", "unknown", ["no metrics.csv in bundle"],
                      ["metrics bundle missing"], "evidence bundle incomplete")
    # Gate B fills in the real signal analysis; the Gate C skeleton below
    # refuses to guess from a single generic symptom (per PRD stop rule).
    return report(
        "ambiguous", "low",
        ["metrics bundle present; signal analysis lands with Gate B telemetry"],
        ["no per-partition lag divergence measured yet"],
        "rule baseline skeleton: insufficient measured signals",
    )


def report(root_cause_family: str, confidence: str, symptoms: list[str],
           counter_evidence: list[str], limitation: str) -> dict[str, Any]:
    return {
        "incident_id": str(uuid.uuid4()),
        "time_window": {"start": "", "end": ""},
        "status": "diagnosed" if confidence in ("high", "medium") else (
            "ambiguous" if root_cause_family != "unknown" else "unknown"),
        "root_cause_family": root_cause_family,
        "confidence": confidence,
        "symptoms": symptoms,
        "evidence": [],
        "counter_evidence": counter_evidence,
        "next_checks": ["collect per-partition lag divergence"],
        "suggested_remediation": ["human review required"],
        "limitations": [limitation],
    }


@click.command()
@click.option("--mode", type=click.Choice(["rules", "agent"]), default="rules")
@click.option("--run", "run_dir", required=True)
def main(mode: str, run_dir: str) -> None:
    bundle = Path(run_dir)
    if mode == "agent":
        click.echo("agent mode requires Gate D + LLM_API_KEY; not implemented yet",
                   err=True)
        raise SystemExit(2)
    incident = diagnose_bundle(bundle)
    (bundle / "incident_rule.json").write_text(json.dumps(incident, indent=2),
                                               encoding="utf-8")
    click.echo(json.dumps(incident, indent=2))


if __name__ == "__main__":
    main()  # pylint: disable=no-value-for-parameter
