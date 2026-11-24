-- orders.events key: order_id. One row per applied business effect.
CREATE TABLE IF NOT EXISTS processed_events (
    event_id        UUID PRIMARY KEY,
    run_id          TEXT NOT NULL,
    order_id        TEXT NOT NULL,
    sequence_number INTEGER NOT NULL,
    event_type      TEXT NOT NULL,
    applied_at_utc  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Current projection per order. Updated atomically with processed_events.
CREATE TABLE IF NOT EXISTS order_projection (
    order_id        TEXT PRIMARY KEY,
    status          TEXT NOT NULL,
    last_sequence   INTEGER NOT NULL,
    last_event_id   UUID NOT NULL,
    updated_at_utc  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Quarantined invalid / non-retryable events. Duplicates possible on crash
-- during DLQ handoff (no exactly-once claim); dedupe by event_id on read.
CREATE TABLE IF NOT EXISTS dlq_records (
    -- TEXT, not UUID: quarantined payloads include undecodable bytes and
    -- synthetic ids (e.g. undecodable-<partition>-<offset>) that are not UUIDs.
    event_id        TEXT NOT NULL,
    run_id          TEXT NOT NULL,
    order_id        TEXT,
    error_class     TEXT NOT NULL,
    raw_payload     JSONB NOT NULL,
    recorded_at_utc TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (event_id, recorded_at_utc)
);

CREATE INDEX IF NOT EXISTS idx_processed_run ON processed_events (run_id);
CREATE INDEX IF NOT EXISTS idx_dlq_run ON dlq_records (run_id);
