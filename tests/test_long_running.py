"""Phase 5: lease renewal and self-fencing, per-handler timeouts and concurrency, loop lag, executors.

Real time is scaled down (lease 1.2 s, renewal 0.4 s) instead of 30 s / 10 s with a fake clock; the ratios
match the defaults (renew every LEASE/3, self-fence below LEASE/3).
"""

from __future__ import annotations

import asyncio
import sqlite3
import time
import uuid
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import pytest

from tests.conftest import LogCapture, make_settings, metric_value
from webhook_receiver.clock import SystemClock
from webhook_receiver.observability.metrics import Metrics
from webhook_receiver.processing.handler import Event, HandlerContext, HandlerSpec
from webhook_receiver.processing.registry import HandlerRegistry
from webhook_receiver.store.base import NewEvent
from webhook_receiver.store.sqlite import SqliteEventStore
from webhook_receiver.worker.dispatcher import Dispatcher
from webhook_receiver.worker.loop_lag import LoopLagMonitor

LEASE = 1.2
RENEW = 0.4


@pytest.fixture
async def store(tmp_path: Path) -> AsyncIterator[SqliteEventStore]:
    s = SqliteEventStore(str(tmp_path / "events.db"))
    await s.open()
    yield s
    await s.close()


async def _add(store: SqliteEventStore, key: str, event_type: str = "t") -> str:
    event_id = str(uuid.uuid7())
    await store.ingest(
        NewEvent(event_id, event_type, key, "h", "null", "req"), SystemClock().now_ms(), 86_400_000
    )
    return event_id


def _settings(tmp_path: Path, **overrides: Any):
    base = {"LEASE_SECONDS": LEASE, "LEASE_RENEW_INTERVAL_SECONDS": RENEW, "POLL_INTERVAL": 0.05}
    base.update(overrides)
    return make_settings(tmp_path, **base)


async def _wait_status(store: SqliteEventStore, event_id: str, status: str, timeout: float = 10) -> Any:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        record = await store.get(event_id)
        if record.status == status:
            return record
        await asyncio.sleep(0.05)
    raise AssertionError(f"{event_id} never reached {status}: {record}")


async def test_long_handler_keeps_its_lease(
    store: SqliteEventStore, tmp_path: Path, logs: LogCapture
) -> None:
    async def long(event: Event, ctx: HandlerContext) -> Any:
        await asyncio.sleep(4 * LEASE)
        return "done"

    async def other(event: Event, ctx: HandlerContext) -> Any:
        raise AssertionError("a second dispatcher must never run this event")

    metrics = Metrics()
    a = Dispatcher(
        store,
        HandlerRegistry(long, HandlerSpec("long", 60, 8)),
        _settings(tmp_path, REPLICA_ID="a"),
        metrics,
        SystemClock(),
    )
    b = Dispatcher(
        store,
        HandlerRegistry(other, HandlerSpec("other", 60, 8)),
        _settings(tmp_path, REPLICA_ID="b"),
        Metrics(),
        SystemClock(),
    )
    event_id = await _add(store, "long")
    await a.start()
    await asyncio.sleep(0.2)
    await b.start()
    try:
        record = await _wait_status(store, event_id, "completed", timeout=15)
    finally:
        await a.stop()
        await b.stop()
    assert record.lease_expiries == 0 and record.attempts == 1
    renewed = metric_value(metrics, "webhook_lease_renewals_total", {"result": "renewed"})
    assert 9 <= renewed <= 13  # ~4.8 s / 0.4 s
    assert not logs.events("lease_reaped")


async def test_renewal_is_chunked(store: SqliteEventStore) -> None:
    for i in range(1200):
        await store.ingest(NewEvent(str(uuid.uuid7()), "t", f"k{i}", "h", "null", "r"), 0, 1)
    claimed = []
    while batch := await store.claim("r1", 500, 1, 30_000):
        claimed.extend(batch)
    assert len(claimed) == 1200
    statements: list[str] = []
    store.writer.set_trace_callback(statements.append)
    renewed = await store.renew("r1", [c.lease_token for c in claimed], 2, 30_000)
    store.writer.set_trace_callback(None)
    assert len(renewed) == 1200
    assert len([s for s in statements if s.lstrip().upper().startswith("UPDATE")]) == 3


async def test_externally_reaped_lease_cancels_handler(
    store: SqliteEventStore, tmp_path: Path, logs: LogCapture
) -> None:
    cancelled = asyncio.Event()

    async def long(event: Event, ctx: HandlerContext) -> Any:
        try:
            await asyncio.sleep(30)
        except asyncio.CancelledError:
            assert ctx.cancelled.is_set()
            cancelled.set()
            raise
        return "should not commit"

    metrics = Metrics()
    dispatcher = Dispatcher(
        store, HandlerRegistry(long, HandlerSpec("long", 60, 8)), _settings(tmp_path), metrics, SystemClock()
    )
    event_id = await _add(store, "k")
    await dispatcher.start()
    try:
        await _wait_status(store, event_id, "processing")
        # Another replica reaps it (as if the lease had expired) and requeues it far in the future.
        conn = sqlite3.connect(store.path, isolation_level=None)
        conn.execute(
            "UPDATE events SET status='queued', lease_token=NULL, lease_owner=NULL, lease_expires_at=NULL, "
            "lease_expiries=lease_expiries+1, next_attempt_at=? WHERE id=?",
            (SystemClock().now_ms() + 60_000, event_id),
        )
        conn.close()
        await asyncio.wait_for(cancelled.wait(), timeout=RENEW * 3)
    finally:
        await dispatcher.stop()
    record = await store.get(event_id)
    assert record.status == "queued" and record.result is None
    assert logs.events("lease_lost")
    assert metric_value(metrics, "webhook_lease_renewals_total", {"result": "lost"}) == 1


async def test_renewal_failure_self_fences_then_another_replica_completes(
    store: SqliteEventStore, tmp_path: Path, logs: LogCapture
) -> None:
    cancelled_at: list[float] = []
    runs = 0

    async def long(event: Event, ctx: HandlerContext) -> Any:
        nonlocal runs
        runs += 1
        # Crash recovery does not consume attempts, so the re-run is attempt 1 too.
        assert ctx.attempt == 1
        if runs == 1:
            try:
                await asyncio.sleep(30)
            except asyncio.CancelledError:
                cancelled_at.append(time.monotonic())
                raise
        return "done"

    store_a = SqliteEventStore(store.path, busy_timeout_ms=50)
    await store_a.open()
    a = Dispatcher(
        store_a,
        HandlerRegistry(long, HandlerSpec("long", 60, 8)),
        _settings(tmp_path, REPLICA_ID="a"),
        Metrics(),
        SystemClock(),
    )
    event_id = await _add(store, "k")
    await a.start()
    blocker = None
    try:
        claimed = await _wait_status(store, event_id, "processing")
        blocker = sqlite3.connect(store.path, isolation_level=None)
        blocker.execute("BEGIN IMMEDIATE")
        locked_at = time.monotonic()
        await asyncio.sleep(LEASE * 1.1)
        assert cancelled_at, "handler was not self-fenced"
        # Self-fenced before the lease could expire.
        remaining_at_cancel = claimed.lease_expires_at / 1000 - (
            time.time() - (time.monotonic() - cancelled_at[0])
        )
        assert remaining_at_cancel > 0
        assert cancelled_at[0] - locked_at < LEASE
        blocker.execute("ROLLBACK")
        blocker.close()
        blocker = None
        await a.stop()
        b = Dispatcher(
            store,
            HandlerRegistry(long, HandlerSpec("long", 60, 8)),
            _settings(tmp_path, REPLICA_ID="b"),
            Metrics(),
            SystemClock(),
        )
        await b.start()
        try:
            record = await _wait_status(store, event_id, "completed", timeout=5)
        finally:
            await b.stop()
    finally:
        if blocker is not None:
            blocker.execute("ROLLBACK")
            blocker.close()
        await store_a.close()
    assert record.lease_expiries == 1 and record.attempts == 1 and runs == 2
    assert logs.events("lease_self_fenced")


async def test_handler_timeout_retries_then_fails(
    store: SqliteEventStore, tmp_path: Path, logs: LogCapture
) -> None:
    deadlines: list[float] = []

    async def hangs(event: Event, ctx: HandlerContext) -> Any:
        deadlines.append(ctx.deadline - asyncio.get_running_loop().time())
        await asyncio.sleep(10)

    metrics = Metrics()
    dispatcher = Dispatcher(
        store,
        HandlerRegistry(hangs, HandlerSpec("hangs", 0.1, 8)),
        _settings(tmp_path, BACKOFF_BASE_SECONDS=0.01),
        metrics,
        SystemClock(),
    )
    event_id = await _add(store, "k")
    await dispatcher.start()
    try:
        record = await _wait_status(store, event_id, "failed")
    finally:
        await dispatcher.stop()
    assert record.failure_reason == "timeout" and record.attempts == 4
    assert record.last_error == "handler exceeded 0.1s"
    assert len(logs.events("handler_timeout")) == 4
    assert len(logs.events("event_retry_scheduled")) == 3
    assert metric_value(metrics, "webhook_handler_timeouts_total", {"handler": "hangs"}) == 4
    assert metric_value(metrics, "webhook_failed_total", {"reason": "timeout"}) == 1
    assert all(0.05 < d <= 0.1 for d in deadlines)


def test_registry_rejects_timeout_above_15_minutes() -> None:
    with pytest.raises(ValueError, match="timeout"):
        HandlerSpec("too-long", 16 * 60, 1)
    HandlerSpec("ok", 15 * 60, 1)


async def test_per_handler_concurrency_and_handler_aware_claiming(
    store: SqliteEventStore, tmp_path: Path
) -> None:
    long_running = 0
    max_long_running = 0
    release_long = asyncio.Event()

    async def long(event: Event, ctx: HandlerContext) -> Any:
        nonlocal long_running, max_long_running
        long_running += 1
        max_long_running = max(max_long_running, long_running)
        try:
            await release_long.wait()
        finally:
            long_running -= 1
        return "long"

    async def short(event: Event, ctx: HandlerContext) -> Any:
        return "short"

    registry = HandlerRegistry(short, HandlerSpec("short", 60, 50))
    registry.register("slow.job", long, HandlerSpec("long", 60, 2))
    metrics = Metrics()
    dispatcher = Dispatcher(store, registry, _settings(tmp_path, LEASE_SECONDS=30), metrics, SystemClock())
    long_ids = [await _add(store, f"l{i}", "slow.job") for i in range(10)]
    short_ids = [await _add(store, f"s{i}", "fast.job") for i in range(10)]
    await dispatcher.start()
    try:
        for event_id in short_ids:
            await _wait_status(store, event_id, "completed")
        conn = sqlite3.connect(store.path)
        processing = conn.execute("SELECT count(*) FROM events WHERE status='processing'").fetchone()[0]
        conn.close()
        assert processing == 2  # never claimed without a free slot
        assert metric_value(metrics, "webhook_handler_inflight", {"handler": "long"}) == 2
        release_long.set()
        for event_id in long_ids:
            await _wait_status(store, event_id, "completed")
    finally:
        release_long.set()
        await dispatcher.stop()
    assert max_long_running == 2


async def test_blocking_handler_shows_up_as_loop_lag(logs: LogCapture) -> None:
    metrics = Metrics()
    monitor = LoopLagMonitor(metrics, interval=0.1, warn_ms=200)
    await monitor.start()
    try:
        await asyncio.sleep(0.15)
        time.sleep(0.5)  # a handler blocking the loop
        await asyncio.sleep(0.25)
    finally:
        await monitor.stop()
    lags = logs.events("event_loop_lag")
    assert lags and lags[0]["lag_ms"] >= 300
    assert metric_value(metrics, "webhook_event_loop_lag_seconds_count") >= 2


async def test_run_blocking_keeps_the_loop_free(logs: LogCapture) -> None:
    from concurrent.futures import ThreadPoolExecutor

    metrics = Metrics()
    monitor = LoopLagMonitor(metrics, interval=0.1, warn_ms=200)
    ctx = HandlerContext("r", "t", 1, 0.0, blocking_executor=ThreadPoolExecutor(2))
    await monitor.start()
    try:
        await asyncio.sleep(0.15)
        await ctx.run_blocking(time.sleep, 0.5)
        await asyncio.sleep(0.15)
    finally:
        await monitor.stop()
    assert not logs.events("event_loop_lag")


def _square(x: int) -> int:
    return x * x


async def test_run_cpu_uses_a_process_pool(store: SqliteEventStore, tmp_path: Path) -> None:
    async def cpu(event: Event, ctx: HandlerContext) -> Any:
        return await ctx.run_cpu(_square, 12)

    dispatcher = Dispatcher(
        store, HandlerRegistry(cpu, HandlerSpec("cpu", 60, 2)), _settings(tmp_path), Metrics(), SystemClock()
    )
    event_id = await _add(store, "k")
    await dispatcher.start()
    try:
        record = await _wait_status(store, event_id, "completed")
    finally:
        await dispatcher.stop()
    assert record.result == "144"


def test_idempotency_key_is_receipt_id() -> None:
    assert HandlerContext("abc", "t", 1, 0.0).idempotency_key == "abc"
