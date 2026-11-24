# Implementation plan (living document — Gate A in progress)

## Build order

1. **Scaffold** (done): layout, pins, compose profiles, Makefile, docs.
2. **Gate A**: `events.py` + `workload.py` (done, unit-tested logic);
   `producer.py` (done); `consumer.py` (done); `db.py` (done);
   `ops/verify.py` oracle (done); integration suite (written, NOT RUN —
   needs Docker daemon + Python).
3. **Gate A verification**: `make up` → `make seed` → run consumers →
   `make verify` → crash/restart + redelivery + F6 hook tests.
4. **Gate B**: metrics exporter wiring, per-partition lag collector,
   fault runner F0–F6, evidence-bundle writer.
5. **Gate C**: rule engine over measured signals (lag divergence,
   DLQ rate, rebalance log, DB latency) + citation validator.
6. **Gate D**: read-only LangGraph investigator, blinded comparison harness.
7. **Gate E**: minimal web view (taste-skill preflight), EVIDENCE.md, demo.

## Acceptance checklist (PRD Gates A–E)

| Gate | Criterion | Status |
|---|---|---|
| A | Pinned Kafka/Postgres, real pipeline, idempotent sink, DLQ | CODE WRITTEN, NOT RUN |
| A | Crash/restart, redelivery, poison, DB-down tests | WRITTEN (skipped), NOT RUN |
| B | Lag/throughput/latency/error/rebalance telemetry | NOT IMPLEMENTED |
| B | F0–F6 reproducible, bundles with SHA + config | NOT IMPLEMENTED |
| C | Rule baseline + IncidentReport incl. ambiguity | SKELETON ONLY |
| D | LangGraph read-only investigator + held-out comparison | NOT IMPLEMENTED |
| E | Minimal UI, EVIDENCE.md, README with real results | NOT IMPLEMENTED |

NOT RUN means: code exists, no passing execution observed in this session.
