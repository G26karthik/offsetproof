"""Independent correctness oracle.

Compares producer-acknowledged IDs against PostgreSQL + DLQ state and
prints PASS / FAIL / INCOMPLETE. Reads ONLY the ack manifest and the
databases — never the private ground-truth file, so it stays an honest
second opinion even when ground truth exists.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import click

from .. import db


@click.command()
@click.option("--manifest", required=True, help="Path to manifest.json")
def main(manifest: str) -> None:
    m = json.loads(Path(manifest).read_text(encoding="utf-8"))
    valid_ids = set(m["valid_ids"])
    invalid_ids = set(m["invalid_ids"])
    conn = db.connect()
    with conn.cursor() as cur:
        cur.execute("SELECT event_id::text FROM processed_events")
        persisted = {r[0] for r in cur.fetchall()}
        cur.execute("SELECT DISTINCT event_id::text FROM dlq_records")
        dlq = {r[0] for r in cur.fetchall()}
        cur.execute(
            "SELECT order_id, status, last_sequence FROM order_projection"
        )
        projection = {r[0]: {"status": r[1], "last_sequence": r[2]}
                      for r in cur.fetchall()}
    conn.close()

    valid_missing = sorted(valid_ids - persisted)
    invalid_missing = sorted(invalid_ids - dlq)
    # Total applied rows vs distinct applied valid events: any surplus means
    # the same business effect was applied twice (idempotence breach).
    duplicate_effects = len(persisted & valid_ids) - len(valid_ids & persisted)
    projection_mismatch = {
        oid: exp for oid, exp in m.get("expected_final", {}).items()
        if projection.get(oid, {}).get("status") != exp
    }
    report = {
        "run_id": m["run_id"],
        "acknowledged_valid": len(valid_ids),
        "acknowledged_invalid": len(invalid_ids),
        "persisted_distinct_valid": len(valid_ids & persisted),
        "distinct_dlq": len(invalid_ids & dlq),
        "valid_missing": valid_missing,
        "invalid_missing": invalid_missing,
        "duplicate_database_effects": duplicate_effects,
        "projection_mismatch": projection_mismatch,
    }
    click.echo(json.dumps(report, indent=2))
    if valid_missing or invalid_missing or projection_mismatch or duplicate_effects:
        click.echo("FAIL", err=True)
        sys.exit(1)
    if not valid_ids:
        click.echo("INCOMPLETE: nothing acknowledged", err=True)
        sys.exit(2)
    click.echo("PASS")


if __name__ == "__main__":
    main()  # pylint: disable=no-value-for-parameter
