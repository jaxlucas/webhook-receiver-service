"""Phase 1 steps 7–8 and Phase 2 step 5: metrics endpoint, health checks, queue gauges."""

from __future__ import annotations

import asyncio
import os
import sqlite3

from prometheus_client.parser import text_string_to_metric_families

from tests.conftest import AppFactory, Harness, metric_value


def _samples(text: str) -> list[tuple[str, dict[str, str], float]]:
    out = []
    for family in text_string_to_metric_families(text):
        for sample in family.samples:
            out.append((sample.name, sample.labels, sample.value))
    return out


def _value(text: str, name: str, **labels: str) -> float:
    for sample_name, sample_labels, value in _samples(text):
        if sample_name == name and all(sample_labels.get(k) == v for k, v in labels.items()):
            return value
    return 0.0


async def test_scrape_shows_exact_counter_deltas(api: Harness) -> None:
    before = (await api.client.get("/metrics")).text
    await api.post_event(key="m1", payload=1)  # accepted
    await api.post_event(key="m1", payload=1)  # duplicate
    await api.post_event(key="m1", payload=2)  # conflict
    await api.post_event(key="m2", event_type="bad type", payload=1)  # validation
    await api.client.post("/events", content=b"x" * 300_000, headers={"content-type": "application/json"})
    after = (await api.client.get("/metrics")).text

    def delta(name: str, **labels: str) -> float:
        return _value(after, name, **labels) - _value(before, name, **labels)

    assert delta("webhook_ingest_total", outcome="accepted") == 1
    assert delta("webhook_ingest_total", outcome="duplicate") == 1
    assert delta("webhook_ingest_total", outcome="conflict") == 1
    assert delta("webhook_ingest_total", outcome="reused_after_ttl") == 0
    assert delta("webhook_rejected_total", reason="validation") == 1
    assert delta("webhook_rejected_total", reason="too_large") == 1
    assert delta("webhook_http_requests_total", route="/events", method="POST", status="202") == 1
    assert delta("webhook_http_requests_total", route="/events", method="POST", status="200") == 1
    assert delta("webhook_http_requests_total", route="/events", method="POST", status="409") == 1
    assert delta("webhook_http_request_duration_seconds_count", route="/events", method="POST") == 4
    # Rejected from Content-Length before routing, so no route template is known.
    assert delta("webhook_http_requests_total", route="unmatched", method="POST", status="413") == 1
    assert _value(after, "webhook_build_info", version="0.1.0", schema_version="1") == 1


async def test_no_label_contains_event_type_or_raw_path(api: Harness) -> None:
    receipt = (await api.post_event(event_type="very.unique.type", payload=1)).json()["receipt_id"]
    await api.client.get(f"/events/{receipt}")
    text = (await api.client.get("/metrics")).text
    for _, labels, _ in _samples(text):
        for value in labels.values():
            assert "very.unique.type" not in value
            assert receipt not in value


async def test_custom_buckets(api: Harness) -> None:
    await api.post_event(payload=1)
    text = (await api.client.get("/metrics")).text
    buckets = {s[1]["le"] for s in _samples(text) if s[0] == "webhook_http_request_duration_seconds_bucket"}
    assert "0.001" in buckets and "2.5" in buckets and "5.0" not in buckets


async def test_metrics_disabled_is_404(harness_factory: AppFactory) -> None:
    async with harness_factory(start_workers=False, METRICS_ENABLED=False) as h:
        response = await h.client.get("/metrics")
        assert response.status_code == 404
        assert response.headers["content-type"].startswith("application/problem+json")


async def test_health_503_when_db_unreadable(harness_factory: AppFactory) -> None:
    async with harness_factory(start_workers=False) as h:
        assert (await h.client.get("/health")).status_code == 200
        for reader in h.store._all_readers:
            await reader.close()
        response = await h.client.get("/health")
        assert response.status_code == 503
        body = response.json()
        assert body["status"] == "error" and body["db"] == "error"
        h.store._all_readers.clear()


async def test_health_503_when_schema_mismatch(harness_factory: AppFactory) -> None:
    async with harness_factory(start_workers=False) as h:
        conn = sqlite3.connect(h.settings.DATABASE_PATH)
        conn.execute("PRAGMA user_version=99")
        conn.commit()
        conn.close()
        response = await h.client.get("/health")
        assert response.status_code == 503
        assert response.json()["schema"] == "error"


async def test_health_503_when_dispatcher_died(harness: Harness) -> None:
    task = harness.services.dispatcher._loop_task
    task.cancel()
    await asyncio.sleep(0.01)
    response = await harness.client.get("/health")
    assert response.status_code == 503
    assert response.json()["dispatcher"] == "error"


async def test_two_scrapes_within_cache_window_run_one_query(api: Harness) -> None:
    cache = api.services.queue_stats
    await api.client.get("/metrics")
    queries = cache.queries
    await api.client.get("/metrics")
    await api.client.get("/health")
    assert cache.queries == queries


async def test_queue_gauges_match_seeded_table(harness_factory: AppFactory) -> None:
    async with harness_factory(start_workers=False, HEALTH_CACHE_SECONDS=0) as h:
        now = h.services.clock.now_ms()
        conn = sqlite3.connect(h.settings.DATABASE_PATH)
        rows = []
        for i, (status, due) in enumerate(
            [
                ("queued", now - 7000),
                ("queued", now - 1000),
                ("queued", now + 60_000),
                ("processing", now),
                ("failed", now),
                ("failed", now),
                ("completed", now),
            ]
        ):
            rows.append((f"0000000{i}", "t", f"k{i}", status, due))
        conn.executemany(
            "INSERT INTO events (id, event_type, idempotency_key, request_hash, payload, status, "
            "next_attempt_at, request_id, received_at, updated_at) VALUES (?, ?, ?, 'h', 'null', ?, ?, 'r', 0, 0)",
            rows,
        )
        conn.commit()
        conn.close()
        text = (await h.client.get("/metrics")).text
        assert _value(text, "webhook_queue_events", status="queued") == 3
        assert _value(text, "webhook_queue_events", status="processing") == 1
        assert _value(text, "webhook_queue_events", status="failed") == 2
        assert 6.5 <= _value(text, "webhook_oldest_queued_age_seconds") <= 9
        assert _value(text, "webhook_wal_bytes") == os.path.getsize(h.settings.DATABASE_PATH + "-wal")
        health = (await h.client.get("/health")).json()
        assert health["queue"]["queued"] == 3 and health["queue"]["failed"] == 2
        assert metric_value(h.metrics, "webhook_worker_slots") == h.settings.WORKER_CONCURRENCY


async def test_bounded_label_series_exist_at_zero(harness: Harness) -> None:
    text = (await harness.client.get("/metrics")).text
    for name, labels in (
        ("webhook_leases_reaped_total", {"outcome": "requeued"}),
        ("webhook_processing_total", {"handler": "stub", "outcome": "failed"}),
        ("webhook_failed_total", {"reason": "timeout"}),
        ("webhook_ingest_total", {"outcome": "reused_after_ttl"}),
    ):
        assert any(
            n == name and all(lbl.get(k) == v for k, v in labels.items()) for n, lbl, _ in _samples(text)
        ), name
