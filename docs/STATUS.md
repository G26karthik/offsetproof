# STATUS — updated per gate. No claim without an observed run.

## Current: Scaffold + Gate A code written, NOT RUN

### Observed runs (all against apache/kafka:4.1.2 + postgres:16, local Docker)
- run-01 baseline orders (seed 42, 2500 orders): 9765/9765 acked, PASS.
- run-03 F1 consumer SIGKILL + restart (seed 44, 800 orders): 3095/3095, PASS.
- run-05 real UCI Online Retail II slice (3000 invoices): 2502 valid
  persisted, 498 real cancels in DLQ, PASS. Dataset SHA256 on record.
- Bugs caught by measurement: cross-run order_id collision (fixed with
  per-run namespace), oracle missing run filter (fixed), sink committing
  rejected-event rows (fixed + regression tests).
- Gate A remainder: poison/F6/DB-down/skew/ledger runs, then Gates B–E.

### Implemented code paths
- `src/offsetproof/events.py` — contract validation, lifecycle state machine.
- `src/offsetproof/workload.py` — seeded generator + replayed expectations.
- `src/offsetproof/producer.py` — acks=all producer + ack manifest + private truth.
- `src/offsetproof/consumer.py` — group, manual commits, idempotent sink, DLQ, F6/F2 hooks.
- `src/offsetproof/db.py` — one-tx apply + dedupe, DLQ record.
- `src/offsetproof/ops/verify.py` — independent oracle (PASS/FAIL/INCOMPLETE).
- `src/offsetproof/ops/readiness.py` — broker/topic/DB gates.
- `src/offsetproof/diagnose.py` — Gate C skeleton (honest unknown).
- `src/offsetproof/faults/__init__.py` — scenario registry + sandbox guard.

### Commands executed
- Version verification via Docker Hub tag API + PyPI metadata (web): PASS.
- `pytest -m unit`: 11 passed, 4 integration deselected (2026-11-24).
- `ruff check src tests`: clean. `mypy src` (strict): clean, 15 files.
- Integration suite + `make up`: NOT RUN (daemon still down).

### Pass/fail + artifacts
- No test executions yet. `results/` empty.

### Unresolved risks
1. No Python interpreter visible in sandbox; no Docker daemon. Blocks all RUN claims.
2. `gh` auth unconfirmed; remote repo not yet created.
3. ponytail skill location unknown; taste preflight pending for Gate E.
4. Consumer/producer/db code unreviewed by execution — expect fixes on first run.

### Next gate
Get Python + Docker daemon → run unit tests → `make up` → Gate A integration.
