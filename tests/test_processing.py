"""Phase 2 steps 3–4: dispatcher, outcomes, retries, crash recovery, graceful shutdown."""

from __future__ import annotations

import asyncio
import random
import sqlite3
import uuid
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import pytest

from tests.conftest import AppFactory, FakeClock, Harness, LogCapture, make_settings, metric_value
from webhook_receiver.observability.metrics import Metrics
from webhook_receiver.processing.errors import PermanentError
from webhook_receiver.processing.handler import Event, HandlerContext, HandlerSpec
from webhook_receiver.processing.registry import HandlerRegistry
from webhook_receiver.store.base import NewEvent
from webhook_receiver.store.sqlite import SqliteEventStore
from webhook_receiver.worker.dispatcher import Dispatcher, backoff_ms


def registry_with(
    handler: Any, name: str = "test", timeout: float = 60, concurrency: int = 128
) -> HandlerRegistry:
    return HandlerRegistry(handler, HandlerSpec(name, timeout, concurrency))


async def drain(dispatcher: Dispatcher) -> None:
    tasks = [inf.task for inf in list(dispatcher._inflight.values()) if inf.task is not None]
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


async def test_posted_event_completes_with_request_id_in_log(harness: Harness) -> None:
    response = await harness.post_event(payload={"a": 1}, headers={"X-Request-ID": "post-req-1"})
    receipt = response.json()["receipt_id"]
    body = await harness.wait_for_status(receipt, {"completed"})
    assert body["attempts"] == 1 and body["result"]["handled_by"] == "stub"
    completed = [line for line in harness.logs.events("event_completed") if line["receipt_id"] == receipt]
    assert completed and completed[0]["request_id"] == "post-req-1"
    assert {"handler_ms", "queue_wait_ms", "end_to_end_ms", "attempts"} <= set(completed[0])
    assert (
        metric_value(harness.metrics, "webhook_processing_total", {"handler": "stub", "outcome": "completed"})
        == 1
    )


async def test_transient_failures_exhaust_retries(harness_factory: AppFactory) -> None:
    async with harness_factory(STUB_FAILURE_RATE=1.0) as h:
        receipt = (await h.post_event(payload=1)).json()["receipt_id"]
        body = await h.wait_for_status(receipt, {"failed"})
        assert body["attempts"] == 4
        assert body["failure_reason"] == "retries_exhausted"
        assert "injected transient failure" in body["last_error"]
        assert body["result"] is None
        assert len(h.logs.events("event_retry_scheduled")) == 3
        assert len(h.logs.events("event_failed")) == 1
        m = h.metrics
        assert metric_value(m, "webhook_processing_total", {"handler": "stub", "outcome": "retry"}) == 3
        assert metric_value(m, "webhook_processing_total", {"handler": "stub", "outcome": "failed"}) == 1
        assert metric_value(m, "webhook_failed_total", {"reason": "retries_exhausted"}) == 1


async def test_permanent_error_fails_at_once(harness_factory: AppFactory) -> None:
    async with harness_factory(STUB_PERMANENT_FAILURE_RATE=1.0) as h:
        receipt = (await h.post_event(payload=1)).json()["receipt_id"]
        body = await h.wait_for_status(receipt, {"failed"})
        assert body["attempts"] == 1
        assert body["failure_reason"] == "permanent_error"
        assert not h.logs.events("event_retry_scheduled")
        assert metric_value(h.metrics, "webhook_failed_total", {"reason": "permanent_error"}) == 1


async def test_transient_then_permanent_skips_remaining_retries(harness_factory: AppFactory) -> None:
    async def handler(event: Event, ctx: HandlerContext) -> Any:
        if ctx.attempt == 1:
            raise ConnectionError("flaky")
        raise PermanentError("bad data")

    async with harness_factory(registry=registry_with(handler)) as h:
        receipt = (await h.post_event(payload=1)).json()["receipt_id"]
        body = await h.wait_for_status(receipt, {"failed"})
        assert body["attempts"] == 2
        assert body["failure_reason"] == "permanent_error"
        assert body["last_error"] == "bad data"


async def test_inflight_gauge_returns_to_zero(harness: Harness) -> None:
    receipts = [(await harness.post_event(key=f"g{i}", payload=i)).json()["receipt_id"] for i in range(20)]
    for receipt in receipts:
        await harness.wait_for_status(receipt, {"completed"})
    await asyncio.sleep(0.05)
    assert metric_value(harness.metrics, "webhook_inflight_handlers") == 0
    assert metric_value(harness.metrics, "webhook_handler_inflight", {"handler": "stub"}) == 0


@pytest.fixture
async def store(tmp_path: Path) -> AsyncIterator[SqliteEventStore]:
    s = SqliteEventStore(str(tmp_path / "events.db"))
    await s.open()
    yield s
    await s.close()


async def _add(store: SqliteEventStore, clock: FakeClock, key: str = "k") -> str:
    event_id = str(uuid.uuid7())
    await store.ingest(NewEvent(event_id, "t", key, "h", "null", "req"), clock.now_ms(), 86_400_000)
    return event_id


def test_backoff_formula() -> None:
    rng = random.Random(1)
    for attempts, base in ((0, 5000), (1, 10_000), (2, 20_000)):
        for _ in range(50):
            assert base * 0.8 <= backoff_ms(5, attempts, rng) <= base * 1.2


async def test_retry_timing_follows_backoff(
    store: SqliteEventStore, tmp_path: Path, logs: LogCapture
) -> None:
    async def failing(event: Event, ctx: HandlerContext) -> Any:
        raise RuntimeError("nope")

    clock = FakeClock()
    settings = make_settings(tmp_path, BACKOFF_BASE_SECONDS=5)
    dispatcher = Dispatcher(store, registry_with(failing), settings, Metrics(), clock)
    event_id = await _add(store, clock)
    for attempts_before, base in ((0, 5000), (1, 10_000), (2, 20_000)):
        await dispatcher.claim()
        await drain(dispatcher)
        record = await store.get(event_id)
        assert record.status == "queued" and record.attempts == attempts_before + 1
        delay = record.next_attempt_at - clock.now_ms()
        assert base * 0.8 <= delay <= base * 1.2, (attempts_before, delay)
        clock.advance((delay - 1) / 1000)
        assert await dispatcher.claim() == 0  # not due yet
        clock.advance(0.001)
    await dispatcher.claim()
    await drain(dispatcher)
    record = await store.get(event_id)
    assert record.status == "failed" and record.attempts == 4


async def test_crash_mid_handler_is_recovered_by_another_dispatcher(
    store: SqliteEventStore, tmp_path: Path, logs: LogCapture
) -> None:
    clock = FakeClock()
    started = asyncio.Event()

    async def hangs(event: Event, ctx: HandlerContext) -> Any:
        started.set()
        await asyncio.Event().wait()

    async def quick(event: Event, ctx: HandlerContext) -> Any:
        return {"ok": True}

    metrics_b = Metrics()
    settings_a = make_settings(tmp_path, REPLICA_ID="replica-a", LEASE_SECONDS=30)
    settings_b = make_settings(tmp_path, REPLICA_ID="replica-b", LEASE_SECONDS=30)
    a = Dispatcher(store, registry_with(hangs), settings_a, Metrics(), clock)
    store_b = SqliteEventStore(store.path)
    await store_b.open()
    try:
        b = Dispatcher(store_b, registry_with(quick), settings_b, metrics_b, clock)
        event_id = await _add(store, clock)
        await a.claim()
        await started.wait()
        # Simulate a crash: the handler dies with no outcome and no release.
        for inf in list(a._inflight.values()):
            inf.task.cancel()
        await drain(a)
        assert (await store.get(event_id)).status == "processing"

        await b.tick()
        assert b.inflight_count == 0  # lease not expired yet
        clock.advance(30)
        await b.tick()
        await drain(b)
        record = await store.get(event_id)
        assert record.status == "completed"
        assert record.attempts == 1 and record.lease_expiries == 1
        assert metric_value(metrics_b, "webhook_leases_reaped_total", {"outcome": "requeued"}) == 1
        reaped = logs.events("lease_reaped")
        assert reaped[0]["previous_owner"] == "replica-a" and reaped[0]["outcome"] == "requeued"
    finally:
        await store_b.close()


async def test_restart_processes_leftover_rows(harness_factory: AppFactory) -> None:
    async with harness_factory(start_workers=False) as h:
        receipts = [(await h.post_event(key=f"l{i}", payload=i)).json()["receipt_id"] for i in range(15)]
    async with harness_factory() as h:
        for receipt in receipts:
            body = await h.wait_for_status(receipt, {"completed"})
            assert body["attempts"] == 1


async def test_shutdown_completes_or_releases_inflight(
    store: SqliteEventStore, tmp_path: Path, logs: LogCapture
) -> None:
    clock = FakeClock()
    release_quick = asyncio.Event()
    seen_cancelled_before_cancel: list[bool] = []

    async def handler(event: Event, ctx: HandlerContext) -> Any:
        if event.idempotency_key.startswith("quick"):
            await release_quick.wait()
            return "done"
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            seen_cancelled_before_cancel.append(ctx.cancelled.is_set())
            raise

    metrics = Metrics()
    settings = make_settings(tmp_path, SHUTDOWN_GRACE=0.3)
    dispatcher = Dispatcher(store, registry_with(handler), settings, metrics, clock)
    quick = [await _add(store, clock, f"quick{i}") for i in range(3)]
    slow = [await _add(store, clock, f"slow{i}") for i in range(2)]
    await dispatcher.start()
    for _ in range(100):
        if dispatcher.inflight_count == 5:
            break
        await asyncio.sleep(0.01)
    assert dispatcher.inflight_count == 5
    stop = asyncio.create_task(dispatcher.stop())
    await asyncio.sleep(0.05)
    release_quick.set()
    await stop

    for event_id in quick:
        record = await store.get(event_id)
        assert record.status == "completed"
    for event_id in slow:
        record = await store.get(event_id)
        assert record.status == "queued" and record.attempts == 0 and record.lease_expiries == 0
        assert record.lease_token is None
    assert seen_cancelled_before_cancel == [True, True]
    assert logs.events("leases_released")[0]["count"] == 2
    assert metric_value(metrics, "webhook_leases_released_total") == 2
    conn = sqlite3.connect(store.path)
    assert conn.execute("SELECT count(*) FROM events WHERE status='processing'").fetchone()[0] == 0
