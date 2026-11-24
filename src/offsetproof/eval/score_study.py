"""Score a frozen set of blinded runs: rules vs agent.

Reads incident_rule.json + incident_agent.json from each bundle dir listed
in a labels file (JSON: {bundle_dir: true_family}), prints per-run lines
and a summary. Missing agent reports count as abstentions, never as hits.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import click

from .compare import score_run, summarize


def load_report(bundle: Path, name: str) -> dict[str, Any] | None:
    path = bundle / name
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    return data if isinstance(data, dict) else None


@click.command()
@click.option("--labels", required=True,
              help="JSON file mapping bundle dir -> true root-cause family")
@click.option("--out", default=None, help="Write summary JSON here")
def main(labels: str, out: str | None) -> None:
    label_map = json.loads(Path(labels).read_text(encoding="utf-8"))
    scores = []
    for bundle_dir, family in label_map.items():
        bundle = Path(bundle_dir)
        rule = load_report(bundle, "incident_rule.json")
        agent = load_report(bundle, "incident_agent.json")
        if rule is None:
            raise SystemExit(f"no rule report in {bundle_dir}; refusing to score")
        s = score_run(family, rule, agent)
        s["bundle"] = bundle_dir
        scores.append(s)
        click.echo(
            f"{bundle_dir}: rule={s['rule_family']}({s['rule_hit']}) "
            f"agent={s['agent_family']}({s['agent_hit']}) "
            f"agent_prec={s['agent_precision']}")
    summary = summarize(scores)
    summary["runs_detail"] = scores
    click.echo(json.dumps({k: v for k, v in summary.items()
                           if k != "runs_detail"}, indent=2))
    if out:
        Path(out).write_text(json.dumps(summary, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()  # pylint: disable=no-value-for-parameter
