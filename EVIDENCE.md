# EVIDENCE — implemented vs measured vs proposed

| run-01 baseline oracle | PASS 9765/9765 | `results/run-01/manifest.json`, seed 42 |
| run-03 F1 crash/restart oracle | PASS 3095/3095 | kill -9 + restart, seed 44 |
| run-05 real-data oracle | PASS 2502 valid + 498 DLQ | UCI Retail II slice, SHA256 `572E3627…67BFB` |
| Sink regression tests | 3 passed, real Postgres | `tests/integration/test_sink.py` |
| Unit tests | VERIFIED 18 passed | `pytest -m unit`, CPython 3.12.16 |
| Lint + types | clean | ruff + mypy strict, 16+ files |

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
