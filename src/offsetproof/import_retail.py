"""Import UCI Online Retail II into the OffsetProof event contract.

Real transactions (Dec 2009 – Dec 2011, UK gift-ware wholesaler), CC BY 4.0,
Chen (2012), DOI 10.24432/C5CG6D. No PII: customer is a 5-digit number.
Mapping is documented, not flattering: normal invoices become ORDER_CREATED,
'C'-prefixed invoices become ORDER_CANCELLED against the base invoice, and
cancels with no matching create in the slice replay to the DLQ — real
messiness, honestly reported by the oracle.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import click

SOURCE_URL = "https://archive.ics.uci.edu/static/public/502/online+retail+ii.zip"
CITATION = "Chen, D. (2012). Online Retail II [Dataset]. UCI Machine Learning Repository. https://doi.org/10.24432/C5CG6D"
LICENSE = "CC BY 4.0"


def zip_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def invoice_key(raw: object) -> tuple[str, bool]:
    s = str(raw)
    if s.startswith("C"):
        return s[1:], True
    return s, False


def to_utc_iso(dt: object, date_str: object) -> str:
    if isinstance(dt, datetime):
        base = dt
    else:
        base = datetime.fromisoformat(str(date_str))
    if base.tzinfo is None:
        # Source timestamps are naive (UK local); interpreted as UTC.
        # Intraday ordering is preserved; absolute times are approximate.
        base = base.replace(tzinfo=timezone.utc)
    return base.isoformat()


@click.command()
@click.option("--xlsx", required=True, help="Path to online_retail_II.xlsx")
@click.option("--zip-path", required=True, help="Original zip (for SHA256)")
@click.option("--sheet", default="Year 2010-2011")
@click.option("--max-invoices", type=int, default=3000)
@click.option("--out-dir", default="data/retail")
def main(xlsx: str, zip_path: str, sheet: str, max_invoices: int, out_dir: str) -> None:
    import openpyxl  # lazy: keeps unit collection dependency-free

    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    wb = openpyxl.load_workbook(xlsx, read_only=True, data_only=True)
    ws = wb[sheet]
    rows = ws.iter_rows(values_only=True)
    header = next(rows)
    idx = {name: i for i, name in enumerate(header)}

    lines: dict[str, list[tuple[Any, ...]]] = defaultdict(list)
    n_rows = 0
    for r in rows:
        n_rows += 1
        inv = r[idx["Invoice"]]
        if inv is None:
            continue
        lines[str(inv)].append(r)
    ordered = sorted(
        lines.items(),
        key=lambda kv: (kv[1][0][idx["InvoiceDate"]], kv[0]),
    )[:max_invoices]

    events = []
    for inv, rs in ordered:
        base, is_cancel = invoice_key(inv)
        first = rs[0]
        total = sum((q or 0) * (p or 0.0) for q, p in
                    ((r[idx["Quantity"]], r[idx["Price"]]) for r in rs))
        order_id = f"retail:{base}"
        if not is_cancel:
            events.append({
                "schema_version": 1,
                "event_id": str(uuid.uuid5(uuid.NAMESPACE_URL, f"retail:{base}:0")),
                "run_id": "",
                "order_id": order_id,
                "sequence_number": 0,
                "event_type": "ORDER_CREATED",
                "event_time_utc": to_utc_iso(first[idx["InvoiceDate"]], first[idx["InvoiceDate"]]),
                "payload": {
                    "lines": len(rs),
                    "total_gbp": round(total, 2),
                    "country": first[idx["Country"]],
                    "source_invoice": str(inv),
                },
            })
        else:
            events.append({
                "schema_version": 1,
                "event_id": str(uuid.uuid5(uuid.NAMESPACE_URL, f"retail:{base}:1")),
                "run_id": "",
                "order_id": order_id,
                "sequence_number": 1,
                "event_type": "ORDER_CANCELLED",
                "event_time_utc": to_utc_iso(first[idx["InvoiceDate"]], first[idx["InvoiceDate"]]),
                "payload": {
                    "lines": len(rs),
                    "total_gbp": round(total, 2),
                    "country": first[idx["Country"]],
                    "source_invoice": str(inv),
                },
            })
    events.sort(key=lambda e: (e["event_time_utc"], e["order_id"]))

    with open(out / "events.jsonl", "w", encoding="utf-8") as fh:
        for e in events:
            fh.write(json.dumps(e) + "\n")
    provenance = {
        "source_url": SOURCE_URL,
        "citation": CITATION,
        "license": LICENSE,
        "zip_sha256": zip_sha256(Path(zip_path)),
        "sheet": sheet,
        "rows_read": n_rows,
        "invoices_seen": len(lines),
        "invoices_kept": len(ordered),
        "events_written": len(events),
        "timestamp_assumption": "naive source datetimes interpreted as UTC",
    }
    (out / "provenance.json").write_text(json.dumps(provenance, indent=2),
                                         encoding="utf-8")
    click.echo(f"invoices={len(ordered)} events={len(events)} rows={n_rows}")


if __name__ == "__main__":
    main()  # pylint: disable=no-value-for-parameter
