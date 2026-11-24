"""Minimal read-only API (Gate E). Real data only; missing signals are 404/empty."""

from __future__ import annotations

from fastapi import FastAPI

app = FastAPI(title="offsetproof")


@app.get("/healthz")
def healthz() -> dict:
    return {"status": "ok", "gate": "A-scaffold"}
