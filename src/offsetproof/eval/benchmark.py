"""Reproducible benchmark runner (Gate B). Plan-driven, artifact-first.

Reads a plan yaml (see eval/plans/), executes control/poison trials
alternately, drains each to its oracle verdict with the same
verify-until-PASS discipline as the demo, snapshots an evidence bundle,
and freezes scorecard.json with measured drain throughput. Fails loudly;
never invents numbers.
"""

from __future__ import annotations

import json
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import click
import yaml

from offsetproof.ops.drain import drain_until_pass, split_logs
from offsetproof.ops.drain import start_consumer, stop_consumer


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
    # top-level eval/plans/<name>.yaml -> plans -> eval -> repo root
    root = Path(config).resolve().parent.parent.parent
    out_dir = Path(out)
    if not out_dir.is_absolute():
        out_dir = root / out_dir
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
    n_trials = int(plan.get("fault_trials", 0)) + int(
        plan.get("control_trials", 0))
    orders = int(plan.get("events_per_trial", 10000)) // 4 or 200
    consumer = start_consumer(root, "localhost:29092",
                              out_dir / "benchmark-consumer.log")
    for trial in range(n_trials):
        seed = seeds[0] + trial
        scenario = "poison" if trial % 2 else "control"
        run_dir = out_dir / f"trial-{trial:03d}"
        produce_cmd = [sys.executable, "-m", "offsetproof.producer",
                       "--seed", str(seed), "--orders", str(orders),
                       "--out-dir", str(run_dir),
                       "--bootstrap", "localhost:29092"]
        if scenario == "poison":
            produce_cmd += ["--poison-every", "37"]
        _sh(produce_cmd, root)
        manifest = json.loads((run_dir / "manifest.json").read_text())
        t0 = time.time()
        verdict = drain_until_pass(run_dir, "localhost:29092", root,
                                   consumer=consumer)
        if verdict != "PASS":
            raise RuntimeError(
                f"trial {trial} ({scenario}) did not reach PASS; "
                "stopping instead of recording a partial trial")
        drain_s = round(time.time() - t0, 1)
        _sh([sys.executable, "-m", "offsetproof.ops.bundle", "--run-dir",
             str(run_dir), "--bootstrap", "localhost:29092"], root)
        split_logs(out_dir / "benchmark-consumer.log", run_dir, manifest)
        _sh([sys.executable, "-m", "offsetproof.diagnose", "--mode", "rules",
             "--run", str(run_dir)], root)
        attempted = int(manifest["attempted"])
        scorecard["trials"].append(
            {"trial": trial, "seed": seed, "scenario": scenario,
             "verdict": verdict, "run_id": manifest["run_id"],
             "drain_seconds": drain_s,
             "events_per_second": round(attempted / drain_s, 1)
             if drain_s > 0 else None})
    stop_consumer(consumer)
    passed = sum(1 for t in scorecard["trials"] if t["verdict"] == "PASS")
    scorecard["status"] = "complete"
    scorecard["summary"] = {"trials": len(scorecard["trials"]),
                            "passed": passed}
    (out_dir / "scorecard.json").write_text(json.dumps(scorecard, indent=2),
                                            encoding="utf-8")
    click.echo(json.dumps(scorecard["summary"]))


if __name__ == "__main__":
    main()  # pylint: disable=no-value-for-parameter
