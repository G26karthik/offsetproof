"""Prometheus application metrics (produce/consume/DB/DLQ)."""

from __future__ import annotations

from prometheus_client import Counter, Histogram, start_http_server

PRODUCED = Counter("offsetproof_produced_total", "Events acknowledged by Kafka", ["run_id"])
CONSUMED = Counter(
    "offsetproof_consumed_total", "Events consumed per outcome", ["run_id", "outcome"]
)
DLQ_RECORDED = Counter("offsetproof_dlq_total", "Events handed to DLQ", ["run_id"])
DB_SECONDS = Histogram("offsetproof_db_seconds", "Postgres transaction latency", ["run_id"])
REBALANCES = Counter("offsetproof_rebalances_total", "Consumer rebalance events", ["group_id"])


def serve(port: int) -> None:
    start_http_server(port)
