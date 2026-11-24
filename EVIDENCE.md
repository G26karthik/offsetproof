# EVIDENCE — implemented vs measured vs proposed

| Item | State | Evidence |
|---|---|---|
| run-01 baseline oracle | PASS 9765/9765 | seed 42 |
| run-03 F1 crash/restart oracle | PASS 3095/3095 | kill -9 + restart, seed 44 |
| run-05 real-data oracle | PASS 2502 valid + 498 DLQ | UCI Retail II slice, SHA256 `572E3627…67BFB` |
| run-08 poison oracle | PASS 2911 valid + 205/205 DLQ | seed 47 |
| run-09 F6 crash-after-commit | PASS 776/776, exit 137 observed | seed 48 |
| run-10 DB-down 45s | PASS after reconnect fix (439 before) | seed 49 |
| run-11 hot partition | PASS; p0 lag 1783 vs ~270 | skew 0.5, seed 50 |
| run-12 ledger profile | PASS 1571/1571 | seed 51 |
| run-15 F5 broker kill | PASS, RF=3/minISR=2, ISR restored | 3-node, seed 54 |
| run-16 slow sink | PASS 394/394 at 0.25s DB delay | seed 55 |
| demo control + poison | PASS/PASS in 35s with bundles | seeds 100/101 |
| Sink regression tests | 4 passed, real Postgres | `tests/integration/test_sink.py` |
| Unit tests | VERIFIED 27 passed (incl. 9 Gate C rules) | `pytest -m unit`, CPython 3.12.16 |
| Lint + types | clean | ruff + mypy strict, 17 files |
| Gate C rule engine | IMPLEMENTED, unit-verified | `diagnose.py`, 9 tests, broker paths deferred |

| Item | State | Evidence |
|---|---|---|
| Kafka `apache/kafka:4.1.2` pinned | IMPLEMENTED (config) | Docker Hub tag API, 2026-11-24, digest `sha256:5cc2a2fd…` |
| `confluent-kafka==2.16.1` pinned | IMPLEMENTED (config) | PyPI metadata, published 2026-10-28 |
| Producer / consumer / sink / DLQ / oracle code | IMPLEMENTED (unrun) | Source in `src/`, no execution yet |
| Unit tests | VERIFIED 11 passed | `pytest -m unit`, CPython 3.12.16, commit of record |
| Integration tests | WRITTEN (skipped), NOT RUN | Needs Docker daemon |
| Fault scenarios F0–F6 measured | PROPOSED | Registry exists; no runs |
| Rule baseline accuracy | PROPOSED | Skeleton returns ambiguous/unknown |
| LangGraph investigator | PROPOSED | Nothing built |
| UI, benchmark, demo | PROPOSED | Nothing built |

Nothing below the config rows may be cited as a result.
