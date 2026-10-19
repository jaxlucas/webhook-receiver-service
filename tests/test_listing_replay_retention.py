"""Phase 2 steps 6–7 (listing, replay) and Phase 3 (key reuse after TTL, purger)."""

from __future__ import annotations

import asyncio
import sqlite3
import uuid
from typing import Any

from tests.conftest import AppFactory, FakeClock, Harness, metric_value
from webhook_receiver.processing.errors import PermanentError
from webhook_receiver.processing.handler import Event, HandlerContext, HandlerSpec
from webhook_receiver.processing.registry import HandlerRegistry


def _seed(db: str, rows: list[tuple[str, str, str, str, int | None]]) -> None:
    """rows: (id, event_type, key, status, completed_at)."""
    conn = sqlite3.connect(db)
    conn.executemany(
        "INSERT INTO events (id, event_type, idempotency_key, request_hash, payload, status, next_attempt_at, "
        "request_id, received_at, updated_at, completed_at, result, last_error, failure_reason) "
        "VALUES (?, ?, ?, 'h', 'null', ?, 0, 'r', 0, 0, ?, "
        "CASE WHEN ?4 = 'completed' THEN '1' END, CASE WHEN ?4 = 'failed' THEN 'err' END, "
        "CASE WHEN ?4 = 'failed' THEN 'retries_exhausted' END)",
        rows,
    )
    conn.commit()
    conn.close()


# ---------------------------------------------------------------- listing


async def test_paging_returns_each_failed_row_once_newest_first(api: Harness) -> None:
    ids = [str(uuid.uuid7()) for _ in range(250)]
    _seed(
        api.settings.DATABASE_PATH,
        [(i, "t" if n % 2 else "u", f"k{n}", "failed", 1) for n, i in enumerate(ids)],
    )
    _seed(api.settings.DATABASE_PATH, [(str(uuid.uuid7()), "t", f"q{n}", "queued", None) for n in range(5)])
    seen: list[str] = []
    cursor = None
    pages = 0
    while True:
        params: dict[str, Any] = {"status": "failed", "limit": 100}
        if cursor:
            params["cursor"] = cursor
        response = await api.client.get("/events", params=params)
        assert response.status_code == 200
        body = response.json()
        pages += 1
        seen.extend(item["receipt_id"] for item in body["items"])
        assert all("result" not in item for item in body["items"])
        assert all(item["status"] == "failed" and item["failure_reason"] for item in body["items"])
        cursor = body["next_cursor"]
        if cursor is None:
            break
    assert pages == 3
    assert seen == sorted(ids, reverse=True)


async def test_listing_event_type_filter(api: Harness) -> None:
    ids = [str(uuid.uuid7()) for _ in range(10)]
    _seed(
        api.settings.DATABASE_PATH,
        [(i, "t" if n % 2 else "u", f"k{n}", "failed", 1) for n, i in enumerate(ids)],
    )
    body = (await api.client.get("/events", params={"status": "failed", "event_type": "t"})).json()
    assert len(body["items"]) == 5 and all(item["event_type"] == "t" for item in body["items"])
    assert body["next_cursor"] is None


async def test_listing_bad_parameters_are_422(api: Harness) -> None:
    for params in (
        {},
        {"status": "nope"},
        {"status": "failed", "limit": 101},
        {"status": "failed", "limit": 0},
        {"status": "failed", "cursor": "garbage!!"},
        {"status": "failed", "cursor": "Zm9v"},
    ):
        response = await api.client.get("/events", params=params)
        assert response.status_code == 422, params
        assert response.json()["type"] == "validation-error"


# ---------------------------------------------------------------- replay


class Switchable:
    def __init__(self) -> None:
        self.permanent = True
        self.calls = 0

    async def __call__(self, event: Event, ctx: HandlerContext) -> Any:
        self.calls += 1
        if self.permanent:
            raise PermanentError("invalid order")
        return {"ok": True}


async def test_replay_of_permanent_failure_completes(harness_factory: AppFactory) -> None:
    handler = Switchable()
    registry = HandlerRegistry(handler, HandlerSpec("switch", 60, 16))
    async with harness_factory(registry=registry) as h:
        receipt = (await h.post_event(payload=1)).json()["receipt_id"]
        await h.wait_for_status(receipt, {"failed"})
        handler.permanent = False
        response = await h.client.post(f"/events/{receipt}/retry", headers={"X-Request-ID": "replay-1"})
        assert response.status_code == 202
        assert response.headers["location"] == f"/events/{receipt}"
        body = response.json()
        assert body["status"] == "queued" and body["attempts"] == 0 and body["replay_count"] == 1
        assert body["failure_reason"] is None
        assert body["last_error"] == "invalid order"  # kept while queued
        done = await h.wait_for_status(receipt, {"completed"})
        assert done["attempts"] == 1 and done["replay_count"] == 1 and done["failure_reason"] is None
        replayed = h.logs.events("event_replayed")[0]
        assert replayed["previous_failure_reason"] == "permanent_error"
        assert replayed["request_id"] == "replay-1"
        assert metric_value(h.metrics, "webhook_replays_total", {"result": "accepted"}) == 1


async def test_replay_of_non_failed_event_is_409_and_changes_nothing(api: Harness) -> None:
    ids = {status: str(uuid.uuid7()) for status in ("completed", "queued", "processing")}
    _seed(
        api.settings.DATABASE_PATH, [(i, "t", s, s, 1 if s == "completed" else None) for s, i in ids.items()]
    )
    for status, event_id in ids.items():
        before = (await api.client.get(f"/events/{event_id}")).json()
        response = await api.client.post(f"/events/{event_id}/retry")
        assert response.status_code == 409
        problem = response.json()
        assert problem["type"] == "event-not-failed" and problem["status"] == status
        assert (await api.client.get(f"/events/{event_id}")).json() == before
    assert len(api.logs.events("event_replay_rejected")) == 3


async def test_replay_unknown_is_404_and_malformed_is_422(api: Harness) -> None:
    assert (await api.client.post(f"/events/{uuid.uuid7()}/retry")).status_code == 404
    assert (await api.client.post("/events/xyz/retry")).status_code == 422
    assert metric_value(api.metrics, "webhook_replays_total", {"result": "not_found"}) == 1


async def test_concurrent_replays_run_handler_once(harness_factory: AppFactory) -> None:
    handler = Switchable()
    registry = HandlerRegistry(handler, HandlerSpec("switch", 60, 16))
    async with harness_factory(registry=registry) as h:
        receipt = (await h.post_event(payload=1)).json()["receipt_id"]
        await h.wait_for_status(receipt, {"failed"})
        handler.permanent = False
        calls_before = handler.calls
        responses = await asyncio.gather(*(h.client.post(f"/events/{receipt}/retry") for _ in range(10)))
        codes = sorted(r.status_code for r in responses)
        assert codes.count(202) == 1 and codes.count(409) == 9
        await h.wait_for_status(receipt, {"completed"})
        await asyncio.sleep(0.1)
        assert handler.calls - calls_before == 1


async def test_replayed_event_is_not_purged(harness_factory: AppFactory) -> None:
    clock = FakeClock()
    async with harness_factory(start_workers=False, clock=clock, IDEMPOTENCY_TTL_SECONDS=60) as h:
        event_id = str(uuid.uuid7())
        _seed(h.settings.DATABASE_PATH, [(event_id, "t", "k", "failed", clock.now_ms() - 120_000)])
        assert (await h.client.post(f"/events/{event_id}/retry")).status_code == 202
        stats = await h.services.purger.run_once()
        assert stats.deleted == 0
        assert (await h.client.get(f"/events/{event_id}")).status_code == 200


# ---------------------------------------------------------------- retention


async def _finish(db: str, receipt: str, status: str, completed_at: int) -> None:
    conn = sqlite3.connect(db)
    conn.execute(
        "UPDATE events SET status = ?, completed_at = ?, failure_reason = CASE WHEN ? = 'failed' "
        "THEN 'retries_exhausted' END WHERE id = ?",
        (status, completed_at, status, receipt),
    )
    conn.commit()
    conn.close()


async def test_key_reuse_after_ttl(harness_factory: AppFactory) -> None:
    clock = FakeClock()
    async with harness_factory(start_workers=False, clock=clock, IDEMPOTENCY_TTL_SECONDS=60) as h:
        db = h.settings.DATABASE_PATH
        first = (await h.post_event(key="a", payload=1)).json()["receipt_id"]
        await _finish(db, first, "completed", clock.now_ms())
        clock.advance(59)
        assert (await h.post_event(key="a", payload=1)).status_code == 200  # finished within TTL
        clock.advance(1)
        same = await h.post_event(key="a", payload=1)
        assert same.status_code == 202 and same.json()["receipt_id"] != first
        assert (await h.client.get(f"/events/{first}")).status_code == 404

        second = (await h.post_event(key="b", payload=1)).json()["receipt_id"]
        await _finish(db, second, "failed", clock.now_ms())
        clock.advance(61)
        different = await h.post_event(key="b", payload={"changed": True})
        assert different.status_code == 202  # expiry wins over the mismatch check

        unfinished = (await h.post_event(key="c", payload=1)).json()["receipt_id"]
        clock.advance(10_000)
        again = await h.post_event(key="c", payload=1)
        assert again.status_code == 200 and again.json()["receipt_id"] == unfinished

        reused = h.logs.events("event_key_reused_after_ttl")
        assert len(reused) == 2 and reused[0]["old_receipt_id"] == first
        assert metric_value(h.metrics, "webhook_ingest_total", {"outcome": "reused_after_ttl"}) == 2


async def test_purger_drains_backlog_in_batches(harness_factory: AppFactory) -> None:
    clock = FakeClock()
    async with harness_factory(
        start_workers=False, clock=clock, IDEMPOTENCY_TTL_SECONDS=60, PURGE_BATCH=1000
    ) as h:
        db = h.settings.DATABASE_PATH
        old = clock.now_ms() - 61_000
        expired = [
            (str(uuid.uuid7()), "t", f"e{n}", "completed" if n % 2 else "failed", old) for n in range(3500)
        ]
        fresh = [(str(uuid.uuid7()), "t", f"f{n}", "completed", clock.now_ms()) for n in range(10)]
        unfinished = [
            (str(uuid.uuid7()), "t", f"u{n}", s, None) for n, s in enumerate(["queued", "processing"])
        ]
        _seed(db, expired + fresh + unfinished)
        stats = await h.services.purger.run_once()
        assert stats.deleted == 3500 and stats.batches == 4
        line = h.logs.events("purge_run")[-1]
        assert line["deleted"] == 3500 and line["batches"] == 4
        assert metric_value(h.metrics, "webhook_purged_total") == 3500
        conn = sqlite3.connect(db)
        assert conn.execute("SELECT count(*) FROM events").fetchone()[0] == 12
        assert (await h.client.get(f"/events/{expired[0][0]}")).status_code == 404
        assert (await h.client.get(f"/events/{fresh[0][0]}")).status_code == 200
