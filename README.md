# OffsetProof

A reproducible Kafka streaming reliability lab with evidence-backed incident
investigation. Synthetic e-commerce order events flow through a real Kafka
group into an idempotent PostgreSQL projection; controlled faults are
injected, recovery is measured, and diagnoses must cite telemetry.

> **Status: under construction (Gate A code written, NOT RUN).**
> Every number in this README will come from measured artifacts in
> `results/` or be marked NOT RUN. See [journey.md](journey.md) for the
> decision log and [docs/STATUS.md](docs/STATUS.md) for gate tracking.

## Quickstart (requires Docker Desktop + Python 3.12)

```bash
cp .env.example .env
make up        # Kafka + Postgres + Prometheus + readiness gates
make seed SEED=42 EVENTS=10000   # deterministic workload + ack manifest
make verify    # oracle: PASS / FAIL / INCOMPLETE
```

Then inject a fault, diagnose, and benchmark:

```bash
make fault SCENARIO=consumer-crash   # explicit opt-in for restarts
make diagnose MODE=rules --run results/latest
make benchmark
```

## Guarantees (tested, not claimed)

- Offsets commit only after the PostgreSQL transaction (or DLQ handoff).
- Redelivery is harmless: same `event_id` → same single effect.
- No end-to-end exactly-once claim; DLQ handoff may duplicate on crash.
- The investigator never sees fault labels or ground truth.

## Layout

- `src/offsetproof/` — producer, consumer, db sink, workload, oracle, rules.
- `sql/` — idempotent schema; `compose*.yaml` — single + 3-broker profiles.
- `docs/` — plan, decisions, status; `eval/` — benchmark plans; `results/`.
