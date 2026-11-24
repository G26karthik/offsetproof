"""Minimal read-only web view (Gate E). Real files only; gaps labeled.

Routes are thin wrappers over pure helpers so the logic is unit-tested
without an HTTP client. The dashboard is one static page, no build step.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse

RESULTS_DIR = Path("results")

app = FastAPI(title="offsetproof")


def list_runs(results_dir: Path = RESULTS_DIR) -> list[dict[str, Any]]:
    """Summaries of runs that have a manifest. Missing dir -> []."""
    runs: list[dict[str, Any]] = []
    if not results_dir.exists():
        return runs
    for manifest in sorted(results_dir.glob("run-*/manifest.json")):
        try:
            m = json.loads(manifest.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        runs.append({
            "run_id": m.get("run_id", manifest.parent.name),
            "profile": m.get("profile", "orders"),
            "seed": m.get("seed"),
            "attempted": m.get("attempted"),
            "acknowledged": m.get("acknowledged"),
            "dataset": bool(m.get("dataset")),
        })
    return runs


def load_run_bundle(
    run_id: str, results_dir: Path = RESULTS_DIR
) -> tuple[dict[str, Any], int]:
    """Return (body, status). Never raises; missing files become 404 JSON."""
    if "/" in run_id or run_id.startswith("."):
        return {"detail": "bad run id"}, 400
    run_dir = results_dir / run_id
    manifest_path = run_dir / "manifest.json"
    if not manifest_path.exists():
        return {"detail": f"unknown run {run_id}"}, 404
    try:
        body: dict[str, Any] = json.loads(
            manifest_path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {"detail": f"unreadable manifest for {run_id}"}, 500
    for name in ("incident_rule.json", "incident_agent.json",
                 "correctness_oracle.json"):
        path = run_dir / name
        if path.exists():
            try:
                body[name.removesuffix(".json")] = json.loads(
                    path.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                body[name.removesuffix(".json")] = {"unavailable": True}
    return body, 200


def read_metrics(run_id: str, limit: int = 500,
                 results_dir: Path = RESULTS_DIR) -> tuple[Any, int]:
    """Parsed metrics.csv rows, capped. Missing file -> ([], 200) with flag."""
    if "/" in run_id or run_id.startswith("."):
        return {"detail": "bad run id"}, 400
    path = results_dir / run_id / "metrics.csv"
    if not path.exists():
        return {"rows": [], "unavailable": True}, 200
    try:
        with open(path, encoding="utf-8", newline="") as fh:
            rows = list(csv.DictReader(fh))[:limit]
    except (OSError, csv.Error, UnicodeDecodeError):
        return {"detail": f"unreadable metrics for {run_id}"}, 500
    return {"rows": rows, "unavailable": False}, 200


def lookup_order(order_id: str) -> tuple[dict[str, Any], int]:
    """Order projection + applied-event count. DB down -> 503, never 500."""
    try:
        from . import db

        conn = db.connect()
    except Exception as exc:  # noqa: BLE001 - DB may be down
        return {"detail": f"database unavailable: {exc}"}, 503
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT order_id, status, last_sequence, last_event_id "
                "FROM order_projection WHERE order_id = %s",
                (order_id,),
            )
            row = cur.fetchone()
            if row is None:
                return {"detail": f"unknown order {order_id}"}, 404
            cur.execute(
                "SELECT COUNT(*) FROM processed_events WHERE order_id = %s",
                (order_id,),
            )
            count_row = cur.fetchone()
            applied = count_row[0] if count_row else 0
    finally:
        conn.close()
    return {
        "order_id": row[0],
        "status": row[1],
        "last_sequence": row[2],
        "last_event_id": str(row[3]),
        "applied_events": applied,
    }, 200


@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/")
def dashboard() -> FileResponse:
    here = Path(__file__).resolve().parent.parent.parent / "web" / "index.html"
    return FileResponse(here)


@app.get("/runs")
def runs() -> list[dict[str, Any]]:
    return list_runs()


@app.get("/runs/{run_id}")
def run_detail(run_id: str) -> JSONResponse:
    body, status = load_run_bundle(run_id)
    return JSONResponse(content=body, status_code=status)


@app.get("/runs/{run_id}/metrics")
def run_metrics(run_id: str) -> JSONResponse:
    body, status = read_metrics(run_id)
    return JSONResponse(content=body, status_code=status)


@app.get("/orders/{order_id}")
def order_detail(order_id: str) -> JSONResponse:
    body, status = lookup_order(order_id)
    return JSONResponse(content=body, status_code=status)
