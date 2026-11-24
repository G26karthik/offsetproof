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


def _drain_until_pass(root: Path, run_dir: Path, bootstrap: str,
                      timeout_rounds: int = 40) -> str:
    """Drain with the continuing main group until the oracle passes.

    A fresh consumer group would replay the whole topic from earliest and
    drain ancient duplicates instead of this run, so every run shares the
    one group and drains to its own oracle verdict. Stray members only
    help: the sink is idempotent, and PASS is checked, not assumed.
    """
    consumer = subprocess.Popen(
        [sys.executable, "-m", "offsetproof.consumer", "--bootstrap",
         bootstrap],
        cwd=root,
        stdout=open(run_dir / "consumer.log", "w", encoding="utf-8"),
        stderr=subprocess.STDOUT,
        text=True,
    )
    verdict = "FAIL"
    try:
        for _ in range(timeout_rounds):
            time.sleep(15)
            out = _sh(
                [sys.executable, "-m", "offsetproof.ops.verify", "--manifest",
                 str(run_dir / "manifest.json")], root)
            if "\nPASS" in out:
                verdict = "PASS"
                break
    finally:
        consumer.terminate()
        try:
            consumer.wait(timeout=30)
        except subprocess.TimeoutExpired:
            consumer.kill()
    return verdict


def _one_run(root: Path, demo: Path, name: str, seed: int, orders: int,
             poison_every: int, bootstrap: str) -> dict[str, Any]:
    run_dir = demo / name
    run_dir.mkdir(parents=True, exist_ok=True)
    poison_flag = ["--poison-every", str(poison_every)] if poison_every else []
    _sh([sys.executable, "-m", "offsetproof.producer", "--seed", str(seed),
         "--orders", str(orders), *poison_flag,
         "--out-dir", str(run_dir), "--bootstrap", bootstrap], root)
    manifest = json.loads((run_dir / "manifest.json").read_text())
    verdict = _drain_until_pass(root, run_dir, bootstrap)
    _sh([sys.executable, "-m", "offsetproof.ops.bundle", "--run-dir",
         str(run_dir), "--bootstrap", bootstrap], root)
    shutil.copy(run_dir / "consumer.log", run_dir / "consumer_logs.jsonl")
    shutil.copy(run_dir / "consumer.log", run_dir / "consumer_logs.jsonl")
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
    control = _one_run(root, demo, "control", 100, 200, 0, bootstrap)
    fault = _one_run(root, demo, "fault-poison", 101, 200, 37, bootstrap)
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
