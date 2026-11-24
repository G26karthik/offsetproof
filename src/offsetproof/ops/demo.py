"""Guided demo: one control run + one poison run, diagnosed and compared.

Runs everything end to end, saves bundles under results/demo/, and prints
a before/fault/recovery style summary with real oracle verdicts. Any step
that fails stops the demo loudly instead of faking a result.
"""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import click

from .drain import drain_until_pass, start_consumer, stop_consumer


def _sh(args: list[str], cwd: Path, env_extra: dict[str, str] | None = None,
        out_file: Path | None = None) -> str:
    import os

    env = dict(os.environ)
    if env_extra:
        env.update(env_extra)
    proc = subprocess.run(args, cwd=cwd, env=env, capture_output=True,
                          text=True, timeout=3600)
    if proc.returncode != 0:
        raise RuntimeError(
            f"{' '.join(args)} failed:\n{(proc.stderr or '')[-2000:]}")
    if out_file is not None:
        out_file.write_text((proc.stdout or "") + (proc.stderr or ""),
                            encoding="utf-8")
    return proc.stdout


def _one_run(root: Path, demo: Path, name: str, seed: int, orders: int,
             poison_every: int, bootstrap: str,
             consumer: "subprocess.Popen[str]") -> dict[str, Any]:
    run_dir = demo / name
    run_dir.mkdir(parents=True, exist_ok=True)
    poison_flag = ["--poison-every", str(poison_every)] if poison_every else []
    _sh([sys.executable, "-m", "offsetproof.producer", "--seed", str(seed),
         "--orders", str(orders), *poison_flag,
         "--out-dir", str(run_dir), "--bootstrap", bootstrap], root)
    manifest = json.loads((run_dir / "manifest.json").read_text())
    verdict = drain_until_pass(run_dir, bootstrap, root, consumer=consumer)
    _sh([sys.executable, "-m", "offsetproof.ops.bundle", "--run-dir",
         str(run_dir), "--bootstrap", bootstrap], root)
    # One consumer serves all runs, so its log is shared. Split this run's
    # records by event_id (commit lines carry it); keep non-commit lines,
    # which describe group state rather than any single run.
    run_ids = set(manifest.get("valid_ids", [])) | set(
        manifest.get("invalid_ids", []))
    with open(demo / "demo-consumer.log", encoding="utf-8") as src:
        kept = [
            line for line in src
            if '"event_id": null' in line
            or any(eid in line for eid in run_ids)
        ]
    (run_dir / "consumer_logs.jsonl").write_text("".join(kept),
                                                 encoding="utf-8")
    _sh([sys.executable, "-m", "offsetproof.diagnose", "--mode", "rules",
         "--run", str(run_dir)], root)
    verify_out = _sh(
        [sys.executable, "-m", "offsetproof.ops.verify", "--manifest",
         str(run_dir / "manifest.json")], root)
    final = "PASS" if verdict == "PASS" and "\nPASS" in verify_out else "FAIL"
    (run_dir / "correctness_oracle.json").write_text(
        json.dumps({"verdict": final, "detail": verify_out[-2000:]}))
    return {"name": name, "run_id": manifest["run_id"], "verdict": final,
            "attempted": manifest["attempted"]}


@click.command()
@click.option("--bootstrap", default="localhost:29092")
@click.option("--out", default="results/demo")
def main(bootstrap: str, out: str) -> None:
    # ops/demo.py -> ops -> offsetproof(pkg) -> src -> repo root
    root = Path(__file__).resolve().parent.parent.parent.parent
    demo = Path(out)
    if not demo.is_absolute():
        demo = root / demo
    if demo.exists():
        shutil.rmtree(demo)
    demo.mkdir(parents=True)
    t0 = time.time()
    consumer = start_consumer(root, bootstrap, demo / "demo-consumer.log")
    try:
        control = _one_run(root, demo, "control", 100, 200, 0, bootstrap,
                           consumer)
        fault = _one_run(root, demo, "fault-poison", 101, 200, 37, bootstrap,
                         consumer)
    finally:
        stop_consumer(consumer)
    dt = time.time() - t0
    summary = {"control": control, "fault": fault,
               "elapsed_seconds": round(dt, 1)}
    (demo / "demo.json").write_text(json.dumps(summary, indent=2))
    click.echo("control: %(verdict)s (%(attempted)s events, %(run_id)s)" % control)
    click.echo("fault:   %(verdict)s (%(attempted)s events, %(run_id)s)" % fault)
    click.echo(f"elapsed: {dt:.0f}s; bundles in {demo}/")
    if control["verdict"] != "PASS" or fault["verdict"] != "PASS":
        raise SystemExit("demo FAILED: see bundles")


if __name__ == "__main__":
    main()  # pylint: disable=no-value-for-parameter
