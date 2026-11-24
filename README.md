# OffsetProof

A reproducible Kafka streaming reliability lab with evidence-backed incident
investigation. Synthetic order events — and optionally a real retail
transaction slice or a synthetic payment ledger — flow through a real Kafka
consumer group into an idempotent PostgreSQL projection. Controlled faults
are injected, recovery is measured against an independent oracle, and every
diagnosis must cite timestamped telemetry.

## Measured results (local Docker, apache/kafka:4.1.2 + postgres:16)

| Run | What | Oracle |
|---|---|---|
| Baseline, 9,765 events | steady load, no fault | PASS, zero missing/phantoms |
| Consumer SIGKILL + restart, 3,095 events | F1 crash recovery | PASS, redelivery idempotent |
| Real UCI Online Retail II slice, 3,000 invoices | messy real data: 2,502 valid, 498 real cancels to DLQ | PASS |
| Poison messages, 3,114 events | 205 invalid quarantined, rest flow | PASS, zero missing/phantoms |
| Crash after DB commit, 776 events | kill between Postgres commit and offset commit | PASS, duplicates were no-ops |

Full history and every bug the measurements caught are in
[journey.md](journey.md); gate status in [docs/STATUS.md](docs/STATUS.md);
what is proven vs planned in [EVIDENCE.md](EVIDENCE.md).

## Why this shape

Kafka teams can see lag but cannot tell a stalled consumer from a slow
database, a hot partition, or poison input — and generic AI explanations
cite no telemetry. OffsetProof's answer has three parts: a detector-ready
telemetry bundle (per-partition lag, DLQ, rebalance, DB latency), a
deterministic rule baseline that says `unknown` when evidence is thin, and
an optional evidence-grounded investigator scored blind against the same
rules. The correctness oracle answers the question dashboards cannot: did
any acknowledged business effect go missing or apply twice?

The failure modes are the ones that matter for money movement
(at-least-once delivery, idempotent ledger posting, reconciliation of every
effect), exercised on synthetic data — no real money, no PII, no employer
systems. A synthetic payment-ledger profile
(`INITIATED→AUTHORIZED→CAPTURED→SETTLED`, with reversal/refund and
double-settle rejection) runs on the same pipeline.

## Quickstart (Docker Desktop + Python 3.12)

```bash
cp .env.example .env
make up        # Kafka + Postgres + Prometheus + readiness gates
make seed SEED=42 EVENTS=10000   # deterministic workload + ack manifest
make verify    # oracle: PASS / FAIL / INCOMPLETE
```

Faults, diagnosis, benchmark, demo:

```bash
make fault SCENARIO=consumer-crash   # explicit opt-in for restarts
make diagnose MODE=rules --run results/latest
make benchmark
```

Web view: `python -m uvicorn offsetproof.api:app` (read-only; missing
signals render as unavailable, never invented).

## Guarantees (tested, not claimed)

- Offsets commit only after the PostgreSQL transaction (or DLQ handoff).
- Redelivery is harmless: same `event_id` → same single effect.
- No end-to-end exactly-once claim; the DLQ path may duplicate on crash
  (counted distinctly by the oracle).
- The investigator never sees fault labels or ground truth; the model is
  advisory and its citations are validated, with `unknown` as a correct
  answer.

## Layout

- `src/offsetproof/` — producer, consumer, sink, workload, oracle, rules,
  agent, API, fault runner, benchmark.
- `sql/` — idempotent schema; `compose*.yaml` — single + 3-broker profiles.
- `docs/` — plan, decisions, status, on-call runbook; `eval/` — plans.
- `results/` (local only) — per-run bundles with manifest, seed, code SHA.
