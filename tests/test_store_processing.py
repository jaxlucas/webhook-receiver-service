"""Phase 2 steps 1–2: stub handler and store claim / reap / fenced outcomes / release."""

from __future__ import annotations

import asyncio
import multiprocessing
import sqlite3
import uuid
from collections.abc import AsyncIterator
from pathlib import Path

import pytest

from webhook_receiver.processing.errors import PermanentError
from webhook_receiver.processing.handler import Event, HandlerContext
from webhook_receiver.processing.stub import StubHandler, StubTransientError
from webhook_receiver.store.base import NewEvent
from webhook_receiver.store.sqlite import SqliteEventStore

NOW = 1_800_000_000_000
LEASE = 30_000


def _ctx() -> HandlerContext:
    return HandlerContext(receipt_id="r", event_type="t", attempt=1, deadline=0.0)


async def test_stub_returns_documented_shape() -> None:
    stub = StubHandler(0, 0)
    result = await stub(Event("r", "order.paid", "k", {"a": 1}, 1), _ctx())
    assert set(result) == {"handled_by", "event_type", "payload_sha256", "duration_ms"}
    assert result["handled_by"] == "stub" and result["event_type"] == "order.paid"
    assert len(result["payload_sha256"]) == 64


async def test_stub_failure_injection() -> None:
    with pytest.raises(StubTransientError):
        await StubHandler(0, 0, failure_rate=1)(Event("r", "t", "k", None, 1), _ctx())
    with pytest.raises(PermanentError):
        await StubHandler(0, 0, permanent_failure_rate=1)(Event("r", "t", "k", None, 1), _ctx())


@pytest.fixture
async def store(tmp_path: Path) -> AsyncIterator[SqliteEventStore]:
    s = SqliteEventStore(str(tmp_path / "e.db"))
    await s.open()
    yield s
    await s.close()


async def _add(store: SqliteEventStore, key: str, now: int = NOW, event_type: str = "t") -> str:
    event_id = str(uuid.uuid7())
    await store.ingest(NewEvent(event_id, event_type, key, "h", "null", "req-" + key), now, 86_400_000)
    return event_id


async def test_claim_respects_next_attempt_at(store: SqliteEventStore) -> None:
    due = await _add(store, "due", NOW - 10)
    await _add(store, "future", NOW + 10_000)
    claimed = await store.claim("r1", 10, NOW, LEASE)
    assert [c.id for c in claimed] == [due]
    assert claimed[0].lease_expires_at == NOW + LEASE
    assert len(claimed[0].lease_token) == 32
    record = await store.get(due)
    assert record.status == "processing" and record.lease_owner == "r1"


async def test_claim_tokens_are_unique_per_row(store: SqliteEventStore) -> None:
    for i in range(20):
        await _add(store, f"k{i}")
    claimed = await store.claim("r1", 20, NOW, LEASE)
    assert len({c.lease_token for c in claimed}) == 20


async def test_reap_requeues_with_jitter_and_keeps_attempts(store: SqliteEventStore) -> None:
    ids = [await _add(store, f"k{i}") for i in range(10)]
    await store.claim("r1", 10, NOW, LEASE)
    assert await store.reap(NOW + LEASE - 1, 10, 5000) == []
    reaped = await store.reap(NOW + LEASE, 10, 5000)
    assert sorted(r.id for r in reaped) == sorted(ids)
    assert all(r.outcome == "requeued" and r.previous_owner == "r1" and r.lease_expiries == 1 for r in reaped)
    due_times = set()
    for event_id in ids:
        record = await store.get(event_id)
        assert record.status == "queued" and record.attempts == 0 and record.lease_expiries == 1
        assert record.lease_token is None and record.lease_owner is None
        assert NOW + LEASE <= record.next_attempt_at < NOW + LEASE + 5000
        due_times.add(record.next_attempt_at)
    assert len(due_times) > 1  # jitter spreads them


async def test_reap_fails_row_at_max_lease_expiries(store: SqliteEventStore) -> None:
    event_id = await _add(store, "poison")
    now = NOW
    for expiry in range(1, 4):
        claimed = await store.claim("r1", 1, now, LEASE)
        assert claimed, f"claim {expiry}"
        now += LEASE
        reaped = await store.reap(now, 3, 1)
        assert reaped[0].outcome == ("failed" if expiry == 3 else "requeued")
        now += 10
    record = await store.get(event_id)
    assert record.status == "failed"
    assert record.failure_reason == "lease_expiries_exhausted"
    assert record.last_error == "lease expired too many times"
    assert record.completed_at is not None and record.attempts == 0 and record.lease_expiries == 3


async def test_outcome_with_stale_token_is_a_noop(store: SqliteEventStore) -> None:
    event_id = await _add(store, "k")
    first = (await store.claim("r1", 1, NOW, LEASE))[0]
    await store.reap(NOW + LEASE, 10, 1)
    second = (await store.claim("r2", 1, NOW + LEASE + 10, LEASE))[0]
    assert await store.complete(event_id, first.lease_token, '"stale"', NOW + LEASE + 20) is None
    assert await store.retry(event_id, first.lease_token, "e", NOW, NOW) is None
    assert await store.fail(event_id, first.lease_token, "e", "permanent_error", NOW) is None
    done = await store.complete(event_id, second.lease_token, '"ok"', NOW + LEASE + 30)
    assert done is not None and done.attempts == 1 and done.status == "completed"
    record = await store.get(event_id)
    assert record.result == '"ok"' and record.lease_expiries == 1


async def test_fenced_outcomes(store: SqliteEventStore) -> None:
    a, b = await _add(store, "a"), await _add(store, "b")
    claims = {c.id: c for c in await store.claim("r1", 2, NOW, LEASE)}
    retried = await store.retry(a, claims[a].lease_token, "boom", NOW + 5000, NOW)
    assert retried.status == "queued" and retried.attempts == 1 and retried.next_attempt_at == NOW + 5000
    failed = await store.fail(b, claims[b].lease_token, "x" * 5000, "permanent_error", NOW)
    assert failed.status == "failed" and failed.attempts == 1
    record = await store.get(b)
    assert record.failure_reason == "permanent_error" and len(record.last_error.encode()) <= 2048
    assert record.completed_at == NOW


async def test_release_changes_no_counter(store: SqliteEventStore) -> None:
    ids = [await _add(store, f"k{i}") for i in range(3)]
    claimed = await store.claim("r1", 3, NOW, LEASE)
    released = await store.release("r1", [c.lease_token for c in claimed[:2]], NOW + 5)
    assert released == 2
    assert await store.release("other-replica", [claimed[2].lease_token], NOW) == 0
    states = [await store.get(i) for i in ids]
    assert sorted(s.status for s in states) == ["processing", "queued", "queued"]
    assert all(s.attempts == 0 and s.lease_expiries == 0 for s in states)


def _claim_in_process(path: str, replica: str, barrier) -> list[str]:
    async def run() -> list[str]:
        store = SqliteEventStore(path)
        await store.open()
        barrier.wait()
        got: list[str] = []
        try:
            for _ in range(30):
                got.extend(c.id for c in await store.claim(replica, 7, NOW, LEASE))
        finally:
            await store.close()
        return got

    return asyncio.run(run())


async def test_two_processes_claiming_never_get_the_same_row(store: SqliteEventStore) -> None:
    for i in range(300):
        await _add(store, f"k{i}")
    ctx = multiprocessing.get_context("spawn")
    with ctx.Manager() as manager:
        barrier = manager.Barrier(2)
        with ctx.Pool(2) as pool:
            a, b = pool.starmap(_claim_in_process, [(store.path, "p1", barrier), (store.path, "p2", barrier)])
    assert not set(a) & set(b)
    assert len(a) + len(b) == 300
    conn = sqlite3.connect(store.path)
    assert conn.execute("SELECT count(*) FROM events WHERE status='processing'").fetchone()[0] == 300
