# Decisions (summary — full rationale in journey.md)

| Date | Decision | Why | Status |
|---|---|---|---|
| 2026-11-24 | Kafka `apache/kafka:4.1.2`, digest-pinned | PRD candidate; verified active on Docker Hub; 4.4.0 (Oct 2026) too fresh for a reliability baseline — recorded as upgrade trial | Pinned in compose |
| 2026-11-24 | `confluent-kafka==2.16.1`, reject 4.4 preview | Stable 2.x/librdkafka 2.16.0; 4.4 preview changes class names/imports | Pinned in pyproject |
| 2026-11-24 | Python 3.12 | PRD floor; best wheel coverage for the pinned client | Declared |
| 2026-11-24 | Manual offset commit after DB/DLQ ack; `enable.auto.commit=false` | Only ordering that prevents commit-past-unfinished-work; crash window covered by idempotent sink + F6 test | Implemented in consumer.py |
| 2026-11-24 | Idempotent sink via `processed_events(event_id)` unique + same-tx transition | At-least-once redelivery + exactly-once *effect*; no end-to-end EOS claim | Implemented in db.py |
| 2026-11-24 | DLQ without exactly-once claim | Crash during DLQ handoff can duplicate; dedupe by event_id on read | Documented + implemented |
| 2026-11-24 | Single-node compose + separate 3-broker resilience profile | No multi-broker claim from a single broker, ever | compose.yaml vs compose.resilience.yaml |
| 2026-11-24 | Oracle reads manifest + DBs only, never private ground truth | Keeps the second opinion honest even when truth exists | Implemented in verify.py |
| 2026-11-24 | Skeleton `diagnose` refuses to guess (`ambiguous`/`unknown`) | PRD stop rule: no cause claim from a single generic symptom | Implemented |
