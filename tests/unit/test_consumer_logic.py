"""Unit tests: retry/backoff/reconnect logic with fakes (no DB, no broker)."""

import time

import pytest

from offsetproof import consumer


@pytest.mark.unit
def test_should_commit_batches_without_weakening_guarantee():
    assert consumer.should_commit(0, 0.0, 100.0) is False
    assert consumer.should_commit(49, 0.0, 1.0) is False
    assert consumer.should_commit(50, 0.0, 1.0) is True
    assert consumer.should_commit(7, 0.0, 5.1) is True
    assert consumer.should_commit(7, 0.0, 4.9) is False


@pytest.mark.unit
def test_backoff_curve_caps_at_thirty():
    assert consumer.backoff_seconds(1) == 2.0
    assert consumer.backoff_seconds(2) == 4.0
    assert consumer.backoff_seconds(5) == 30.0
    assert consumer.backoff_seconds(50) == 30.0


@pytest.mark.unit
def test_resilient_connection_resets_on_demand():
    made = []

    class FakeConn:
        def __init__(self):
            self.closed = False

        def close(self):
            self.closed = True

    def factory():
        c = FakeConn()
        made.append(c)
        return c

    store = consumer.ResilientConnection(connect=factory)
    first = store.conn
    store.reset()
    assert first.closed
    assert store.conn is not first
    assert len(made) == 2


@pytest.mark.unit
def test_with_retries_succeeds_after_transient_failures(monkeypatch):
    monkeypatch.setattr(time, "sleep", lambda s: None)
    calls = {"n": 0}

    class FakeConn:
        def close(self):
            pass

    store = consumer.ResilientConnection(connect=FakeConn)

    def op(conn):
        calls["n"] += 1
        if calls["n"] < 3:
            raise ConnectionError("down")
        return "ok"

    assert consumer.with_retries(store, op, "probe") == "ok"
    assert calls["n"] == 3


@pytest.mark.unit
def test_with_retries_exhausts_and_raises(monkeypatch):
    monkeypatch.setattr(time, "sleep", lambda s: None)
    monkeypatch.setattr(consumer, "TRANSIENT_RETRY_LIMIT", 2)
    attempts = {"n": 0}

    class FakeConn:
        def close(self):
            pass

    store = consumer.ResilientConnection(connect=FakeConn)

    def op(conn):
        attempts["n"] += 1
        raise ConnectionError("still down")

    with pytest.raises(ConnectionError):
        consumer.with_retries(store, op, "probe")
    assert attempts["n"] == 3  # initial + 2 retries, then raise
