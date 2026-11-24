"""Shared drain harness: one consumer per session, verify-until-PASS.

A fresh consumer group would replay the whole topic from earliest and
drain ancient duplicates instead of the current run, so every run shares
the one group and drains to its own oracle verdict. Killing and
restarting a consumer between runs forces a group rebalance every time,
and a fresh join right after a kill can stall past any fixed timeout
while the coordinator fences the dead member (observed on the resilience
profile). One long-lived consumer per session avoids the pathology
entirely; the idempotent sink keeps redeliveries harmless.
"""

from __future__ import annotations

import subprocess
import sys
import time
from pathlib import Path


def start_consumer(root: Path, bootstrap: str,
                   log_path: Path) -> "subprocess.Popen[str]":
    """Launch one drain consumer. Share it across runs in a session."""
    return subprocess.Popen(
        [sys.executable, "-m", "offsetproof.consumer", "--bootstrap",
         bootstrap],
        cwd=root,
        stdout=open(log_path, "w", encoding="utf-8"),
        stderr=subprocess.STDOUT,
        text=True,
    )


def stop_consumer(consumer: "subprocess.Popen[str]") -> None:
    try:
        consumer.terminate()
        consumer.wait(timeout=30)
    except subprocess.TimeoutExpired:
        consumer.kill()


def drain_until_pass(run_dir: Path, bootstrap: str, root: Path,
                     timeout_rounds: int = 40,
                     consumer: "subprocess.Popen[str] | None" = None,
                     verify_module: str = "offsetproof.ops.verify") -> str:
    """Poll the oracle until this run passes. Returns PASS or FAIL."""
    import subprocess as _sp

    owned = consumer is None
    if owned:
        consumer = start_consumer(root, bootstrap, run_dir / "consumer.log")
    assert consumer is not None
    verdict = "FAIL"
    try:
        for _ in range(timeout_rounds):
            time.sleep(15)
            proc = _sp.run(
                [sys.executable, "-m", verify_module, "--manifest",
                 str(run_dir / "manifest.json")],
                cwd=root, capture_output=True, text=True, timeout=300,
            )
            if proc.returncode == 0:
                verdict = "PASS"
                break
    finally:
        if owned:
            stop_consumer(consumer)
    return verdict
