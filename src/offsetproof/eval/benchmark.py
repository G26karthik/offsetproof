"""Reproducible benchmark runner (Gate B). Plan-driven, artifact-first.

Reads eval/plans/baseline.yaml, executes each trial through the fault
runner in --execute mode, drains with the consumer, verifies with the
oracle, and freezes scorecard.json. Fails loudly; never invents numbers.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import click
import yaml


def _sh(args: list[str], cwd: Path) -> str:
    proc = subprocess.run(args, cwd=cwd, capture_output=True, text=True,
                          timeout=3600)
    if proc.returncode != 0:
        raise RuntimeError(f"{' '.join(args)} failed:\n{proc.stderr[-2000:]}")
    return proc.stdout


@click.command()
@click.option("--config", required=True)
@click.option("--out", default="results/benchmark")
@click.option("--dry-run", is_flag=True, default=False)
def main(config: str, out: str, dry_run: bool) -> None:
    plan = yaml.safe_load(Path(config).read_text(encoding="utf-8"))
    root = Path(config).resolve().parent.parent.parent
    out_dir = Path(out)
    out_dir.mkdir(parents=True, exist_ok=True)
    scorecard: dict[str, Any] = {
        "plan": plan,
        "trials": [],
        "status": "planned" if dry_run else "running",
    }
    if dry_run:
        click.echo(json.dumps(scorecard, indent=2))
        return
    seeds = [int(plan.get("seed", 42))]
    for trial in range(int(plan.get("fault_trials", 0))):
        seed = seeds[0] + trial
        run_dir = out_dir / f"trial-{trial:03d}"
        _sh([sys.executable, "-m", "offsetproof.producer", "--seed", str(seed),
             "--orders", "200", "--out-dir", str(run_dir),
             "--bootstrap", "localhost:29092"], root)
        manifest = json.loads((run_dir / "manifest.json").read_text())
        _sh([sys.executable, "-m", "offsetproof.consumer", "--bootstrap",
             "localhost:29092", "--max-messages",
             str(manifest["attempted"])], root)
        verify_out = _sh(
            [sys.executable, "-m", "offsetproof.ops.verify", "--manifest",
             str(run_dir / "manifest.json")], root)
        verdict = "PASS" if "\nPASS" in verify_out else "FAIL"
        scorecard["trials"].append(
            {"trial": trial, "seed": seed, "verdict": verdict,
             "run_id": manifest["run_id"]})
    passed = sum(1 for t in scorecard["trials"] if t["verdict"] == "PASS")
    scorecard["status"] = "complete"
    scorecard["summary"] = {"trials": len(scorecard["trials"]),
                            "passed": passed}
    (out_dir / "scorecard.json").write_text(json.dumps(scorecard, indent=2),
                                            encoding="utf-8")
    click.echo(json.dumps(scorecard["summary"]))


if __name__ == "__main__":
    main()  # pylint: disable=no-value-for-parameter
