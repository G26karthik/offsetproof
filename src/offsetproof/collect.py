"""Live telemetry collection (Gate B). Thin live wrapper, pure core.

`build_snapshot` takes plain offset maps and is fully unit-testable.
`collect_live` talks to the broker and is covered by integration runs,
not by unit tests.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from confluent_kafka import ConsumerGroupTopicPartitions, TopicPartition
from confluent_kafka.admin import AdminClient
from confluent_kafka.admin import OffsetSpec  # type: ignore[attr-defined]

from .observe import compute_lag, summarize_lag


def build_snapshot(
    end_offsets: dict[int, int],
    committed_offsets: dict[int, int],
    group_id: str,
    topic: str,
    timestamp_utc: str | None = None,
) -> dict[str, Any]:
    """Assemble one sampled telemetry row from plain offset maps."""
    lag = compute_lag(end_offsets, committed_offsets)
    return {
        "timestamp_utc": timestamp_utc
        or datetime.now(timezone.utc).isoformat(),
        "topic": topic,
        "group_id": group_id,
        **{f"p{p}_lag": v for p, v in lag.items()},
        "lag_total": sum(v for v in lag.values() if v is not None),
        **{
            f"lag_summary_{k}": v
            for k, v in summarize_lag(lag).items()
            if not isinstance(v, list)
        },
    }


def collect_live(bootstrap: str, topic: str, group_id: str) -> dict[str, Any]:
    """Sample end offsets and committed group offsets from a live broker.

    Every Admin API call below was verified against confluent-kafka 2.16.1
    signatures without a broker; the result-object attribute reads
    (.topic_partitions, .offset) are asserted loudly and the whole function
    is integration-covered, never silently defaulted.
    """
    admin = AdminClient({"bootstrap.servers": bootstrap})
    meta = admin.list_topics(topic=topic, timeout=30)
    parts = sorted(meta.topics[topic].partitions)

    # latest() is a C-extension static with no stubs; verified at runtime.
    latest = OffsetSpec.latest()  # type: ignore[no-untyped-call]
    end_futures = admin.list_offsets(
        {TopicPartition(topic, p): latest for p in parts}
    )
    end_offsets: dict[int, int] = {}
    for tp, fut in end_futures.items():
        info = fut.result(timeout=30)
        offset = getattr(info, "offset", None)
        if not isinstance(offset, int):
            raise RuntimeError(
                f"unexpected list_offsets result for {topic}/{tp.partition}: "
                f"{info!r}"
            )
        end_offsets[tp.partition] = offset

    group_futures = admin.list_consumer_group_offsets(
        [ConsumerGroupTopicPartitions(group_id)]
    )
    group_state = group_futures[group_id].result(timeout=30)
    tps = group_state.topic_partitions or []
    if isinstance(tps, dict):
        tps = list(tps.values())
    committed = {
        tp.partition: tp.offset
        for tp in tps
        if isinstance(getattr(tp, "offset", None), int) and tp.offset >= 0
    }
    return build_snapshot(end_offsets, committed, group_id, topic)
