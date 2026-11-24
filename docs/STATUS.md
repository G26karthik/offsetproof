# STATUS — updated per gate. No claim without an observed run.

## Current: Scaffold + Gate A code written, NOT RUN

### Observed runs (all against apache/kafka:4.1.2 + postgres:16, local Docker)
- run-01 baseline orders (seed 42, 2500 orders): 9765/9765 acked, PASS.
- run-03 F1 consumer SIGKILL + restart (seed 44, 800 orders): 3095/3095, PASS.
- run-05 real UCI Online Retail II slice (3000 invoices): 2502 valid
  persisted, 498 real cancels in DLQ, PASS. Dataset SHA256 on record.
- run-06 poison (seed 45): SUPERSEDED — operator truncated tables mid-run
  with offsets committed, destroying its evidence. Lesson encoded:
  integration fixture now deletes only its own run_id rows, never TRUNCATE.
- run-07 poison rerun: exposed DLQ identity bug (below); evidence kept.
- run-08 poison (seed 47, 800 orders): 2911 valid + 205/205 DLQ, PASS.
- run-09 F6 crash-after-DB-commit (seed 48): exit 137, 776/776 PASS.
- run-10 DB-down 45s (seed 49): pre-fix code never reconnected (439/1558);
  with ResilientConnection the remainder drained to full PASS.
- run-11 hot partition (seed 50, skew 0.5): partition 0 lag 1,783 vs
  ~270 elsewhere; 1582 valid + 1550 invalid all reconciled, PASS.
  (Hot key replays concatenated lifecycles, so its repeats are
  expected-invalid — volume skew is real, quarantine is correct.)
- run-12 payment ledger (seed 51, profile ledger): 1571/1571 PASS.
- Bugs caught by measurement: cross-run order_id collision (per-run
  namespace), oracle run filter, sink phantom rows (exception rollback),
  UUID-only DLQ column (TEXT), DLQ run attribution (envelope run_id),
  DLQ identity (envelope event_id; synthetic undecodable-* ids hid 84
  poison records from the oracle in run-06/07).
- Gate A remainder: F6 verdict, DB-down, skew, ledger runs, then B–E.
- 2026-11-24: user granted blanket permission to proceed autonomously
  (docker/gh/installs). Sole-contributor rule stands: no co-author
  trailers, only user identity on commits.

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
