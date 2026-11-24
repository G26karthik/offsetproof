"""Fault-runner CLI (Gate B). Orchestrates scenarios, never leaks truth.

Every run writes its scenario + exact commands to results/private/<run>/
via record_fault. That directory is never on the investigator's input
path. Destructive actions require --i-understand and only ever touch
containers starting with "offsetproof-". Default is --dry-run: print the
manual commands instead of executing them.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import click

from . import _guard_container, docker, record_fault, REGISTRY


def _split_env(parts: list[str]) -> tuple[dict[str, str], list[str]]:
    env: dict[str, str] = {}
    rest = list(parts)
    while rest and "=" in rest[0] and not rest[0].startswith("-"):
        key, _, value = rest.pop(0).partition("=")
        env[key] = value
    return env, rest


def _run_step(cmd: list[str], dry_run: bool) -> None:
    if dry_run:
        click.echo("+ " + " ".join(cmd))
        return
    if cmd[0] == "docker":
        proc = docker(*cmd[1:])  # text=True: stdout/stderr are already str
        out, err, code = proc.stdout, proc.stderr, proc.returncode
    elif cmd[0] == "python":
        import os
        import subprocess

        extra_env, rest = _split_env(cmd[1:])
        if not rest or rest[0] != "-m":
            raise click.ClickException(
                f"python commands must use -m module form: {cmd}")
        merged = dict(os.environ)
        merged.update(extra_env)
        proc2 = subprocess.run(
            [sys.executable, *rest],
            env=merged,
            capture_output=True,
            timeout=3600,
        )
        out, err, code = (
            proc2.stdout.decode()[-2000:],
            proc2.stderr.decode()[-2000:],
            proc2.returncode,
        )
    else:
        raise click.ClickException(f"refusing to run non-docker/python command: {cmd}")
    if code != 0:
        raise click.ClickException(f"command failed: {' '.join(cmd)}\n{err[:2000]}")
    click.echo(out[-2000:] if out else "(ok)")


@click.command()
@click.option("--scenario", type=click.Choice(sorted(REGISTRY)),
              required=True)
@click.option("--run-dir", required=True, help="results/run-XX for artifacts")
@click.option("--bootstrap", default="localhost:29092")
@click.option("--execute", is_flag=True, default=False,
              help="Actually run docker commands (default prints them)")
@click.option("--i-understand", is_flag=True, default=False,
              help="Required opt-in for destructive scenarios")
def main(scenario: str, run_dir: str, bootstrap: str, execute: bool,
         i_understand: bool) -> None:
    spec = REGISTRY[scenario]
    dry_run = not execute
    if spec.destructive and execute and not i_understand:
        raise click.ClickException(
            f"scenario {scenario!r} is destructive: pass --i-understand"
        )
    run_path = Path(run_dir)
    run_path.mkdir(parents=True, exist_ok=True)
    commands: list[str] = []

    if scenario == "consumer-crash":
        target = _guard_container("offsetproof-consumer-1")
        commands = [
            f"docker kill -s SIGKILL {target}",
            f"docker start {target}",
        ]
    elif scenario == "broker-restart":
        target = _guard_container("offsetproof-kafka-1")
        commands = [
            f"docker restart {target}",
        ]
    elif scenario == "slow-sink":
        commands = [
            "OFFSETPROOF_DB_DELAY_SECONDS=0.25 "
            "python -m offsetproof.consumer --bootstrap " + bootstrap,
        ]
    elif scenario == "crash-after-commit":
        commands = [
            "OFFSETPROOF_CRASH_AFTER_DB_COMMIT=1 "
            "python -m offsetproof.consumer --bootstrap " + bootstrap,
        ]
    elif scenario == "poison":
        commands = [
            f"python -m offsetproof.producer --poison-every 37 "
            f"--out-dir {run_dir} --bootstrap {bootstrap}",
        ]
    elif scenario == "hot-partition":
        commands = [
            f"python -m offsetproof.producer --skew-hot-fraction 0.5 "
            f"--out-dir {run_dir} --bootstrap {bootstrap}",
        ]
    else:  # normal
        commands = [
            f"python -m offsetproof.producer --out-dir {run_dir} "
            f"--bootstrap {bootstrap}",
        ]

    for cmd in commands:
        parts = cmd.split()
        _run_step(parts, dry_run)
    fault_path = record_fault(run_path, spec, commands)
    click.echo(json.dumps({"scenario": scenario, "dry_run": dry_run,
                           "fault_record": str(fault_path)}))


if __name__ == "__main__":
    main()  # pylint: disable=no-value-for-parameter
