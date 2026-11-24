# EVIDENCE — implemented vs measured vs proposed

| Item | State | Evidence |
|---|---|---|
| Kafka `apache/kafka:4.1.2` pinned | IMPLEMENTED (config) | Docker Hub tag API, 2026-11-24, digest `sha256:5cc2a2fd…` |
| `confluent-kafka==2.16.1` pinned | IMPLEMENTED (config) | PyPI metadata, published 2026-10-28 |
| Producer / consumer / sink / DLQ / oracle code | IMPLEMENTED (unrun) | Source in `src/`, no execution yet |
| Unit tests | WRITTEN, NOT RUN | `tests/unit/test_events.py` — no interpreter in session |
| Integration tests | WRITTEN (skipped), NOT RUN | Needs Docker daemon |
| Fault scenarios F0–F6 measured | PROPOSED | Registry exists; no runs |
| Rule baseline accuracy | PROPOSED | Skeleton returns ambiguous/unknown |
| LangGraph investigator | PROPOSED | Nothing built |
| UI, benchmark, demo | PROPOSED | Nothing built |

Nothing below the config rows may be cited as a result.
