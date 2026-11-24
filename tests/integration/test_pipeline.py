"""Integration: real Kafka + Postgres pipeline (needs Docker daemon)."""

import pytest

pytestmark = pytest.mark.integration


def test_produce_consume_oracle_passes():
    pytest.skip("NOT RUN: Docker daemon unavailable in this session")


def test_crash_restart_no_missing_events():
    pytest.skip("NOT RUN: Docker daemon unavailable in this session")


def test_poison_dlq_and_continuation():
    pytest.skip("NOT RUN: Docker daemon unavailable in this session")


def test_crash_after_db_commit_idempotent():
    pytest.skip("NOT RUN: Docker daemon unavailable in this session")
