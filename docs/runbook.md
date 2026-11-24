# OffsetProof on-call runbook (curated, evidence-scoped)

These notes are the only prose the investigator may quote. Every entry
describes what to measure, not what to conclude.

## Lag

- Consumer lag is log-end offset minus committed group offset, per partition.
- Lag climbing on every partition with zero commits means no progress.
- Lag concentrated on one partition (about 80% or more of the total) means
  the key mapping or that partition's processing deserves attention.
- A producer burst also raises lag; check produce rate before blaming the sink.

## Slow sink

- Compare DB write-latency percentiles against broker produce latency in the
  same window before calling the database slow.
- Commits advancing while total lag does not drain fits a slow sink, a hot
  partition, poison input, or a burst. One symptom never convicts alone.

## Poison input

- Invalid or undecodable records must appear in the DLQ with an error class.
- DLQ handoff may duplicate after a crash; count distinct event IDs.
- A poison batch does not explain lag elsewhere; keep counting partitions.

## Rebalance and membership

- Assignment and revocation events come from consumer logs, not guesses.
- After a crash, redelivery is normal; duplicates must be idempotent no-ops.
- Never commit past an unprocessed valid event; offsets follow DB writes.
