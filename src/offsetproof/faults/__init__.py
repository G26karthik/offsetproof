"""Fault-injection registry. Sandboxed to this repo's Compose project only.

Every scenario records its commands + labels into the evaluator artifact
(results/private/<run_id>/fault.json). That file is NEVER on the
investigator's input path. Destructive actions require explicit opt-in and
only ever target containers whose names start with "offsetproof-".
"""

from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass
from pathlib import Path

ALLOWED_PREFIX = "offsetproof-"

SCENARIOS = (
    "normal",          # F0: steady traffic / burst / varied keys, no fault
    "consumer-crash",  # F1: kill one worker container, then restart
    "slow-sink",       # F2: app-level DB delay (evaluator-side env only)
    "poison",          # F3: invalid records interleaved (producer --poison-every)
    "hot-partition",   # F4: skewed keys (--skew-hot-fraction)
    "broker-restart",  # F5: resilience profile only, one broker restart
    "crash-after-commit",  # F6: OFFSETPROOF_CRASH_AFTER_DB_COMMIT=1 test hook
)


@dataclass(frozen=True)
class FaultSpec:
    scenario: str
    destructive: bool
    description: str


REGISTRY = {
    "normal": FaultSpec("normal", False, "Baseline load, no injected fault"),
    "consumer-crash": FaultSpec("consumer-crash", True, "SIGKILL one worker, restart it"),
    "slow-sink": FaultSpec("slow-sink", False, "DB delay injected via worker env"),
    "poison": FaultSpec("poison", False, "Invalid records between valid ones"),
    "hot-partition": FaultSpec("hot-partition", False, "Key skew to one partition"),
    "broker-restart": FaultSpec("broker-restart", True, "Restart one broker (3-node profile)"),
    "crash-after-commit": FaultSpec("crash-after-commit", True, "Kill worker after DB commit, pre-offset-commit"),
}


def _guard_container(name: str) -> str:
    if not name.startswith(ALLOWED_PREFIX):
        raise ValueError(f"refusing to touch non-project container: {name}")
    return name


def docker(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(["docker", *args], capture_output=True, text=True, timeout=120)


def record_fault(run_dir: Path, spec: FaultSpec, commands: list[str]) -> Path:
    """Write the evaluator-side fault record (private, never for diagnosis)."""
    private = Path("results") / "private" / run_dir.name / "fault.json"
    private.parent.mkdir(parents=True, exist_ok=True)
    private.write_text(
        json.dumps(
            {"scenario": spec.scenario, "destructive": spec.destructive,
             "commands": commands, "description": spec.description},
            indent=2,
        ),
        encoding="utf-8",
    )
    return private
