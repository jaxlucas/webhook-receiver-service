"""Prometheus metric definitions. One registry per app instance, so tests never share counters.

``event_type`` is client-controlled and is never a label; per-handler metrics use the registry name.
"""

from __future__ import annotations

from collections.abc import Iterator

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram
from prometheus_client.core import GaugeMetricFamily
from prometheus_client.registry import Collector

HTTP_BUCKETS = (0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5)
HANDLER_BUCKETS = (0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600, 900)
LAG_BUCKETS = (0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5)
QUEUE_BUCKETS = (0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300)


class Metrics:
    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        self.registry = registry or CollectorRegistry(auto_describe=True)
        r = self.registry

        self.http_requests = Counter(
            "webhook_http_requests_total", "HTTP requests", ["route", "method", "status"], registry=r
        )
        self.http_duration = Histogram(
            "webhook_http_request_duration_seconds",
            "HTTP request duration",
            ["route", "method"],
            buckets=HTTP_BUCKETS,
            registry=r,
        )
        self.ingest = Counter("webhook_ingest_total", "Ingest outcomes", ["outcome"], registry=r)
        self.rejected = Counter("webhook_rejected_total", "Rejected requests", ["reason"], registry=r)
        self.processing = Counter(
            "webhook_processing_total", "Processing outcomes", ["handler", "outcome"], registry=r
        )
        self.failed = Counter("webhook_failed_total", "Events that reached failed", ["reason"], registry=r)
        self.replays = Counter("webhook_replays_total", "Manual replays", ["result"], registry=r)
        self.handler_duration = Histogram(
            "webhook_handler_duration_seconds",
            "Handler run time",
            ["handler"],
            buckets=HANDLER_BUCKETS,
            registry=r,
        )
        self.lease_renewals = Counter(
            "webhook_lease_renewals_total", "Lease renewal results", ["result"], registry=r
        )
        self.handler_timeouts = Counter(
            "webhook_handler_timeouts_total", "Handler timeouts", ["handler"], registry=r
        )
        self.handler_inflight = Gauge(
            "webhook_handler_inflight", "Running handlers per handler", ["handler"], registry=r
        )
        self.loop_lag = Histogram(
            "webhook_event_loop_lag_seconds", "Event-loop scheduling delay", buckets=LAG_BUCKETS, registry=r
        )
        self.queue_wait = Histogram(
            "webhook_queue_wait_seconds",
            "Due (received or re-queued) to claimed",
            buckets=QUEUE_BUCKETS,
            registry=r,
        )
        self.end_to_end = Histogram(
            "webhook_end_to_end_seconds", "Received to completed", buckets=QUEUE_BUCKETS, registry=r
        )
        self.leases_reaped = Counter(
            "webhook_leases_reaped_total", "Expired leases reaped", ["outcome"], registry=r
        )
        self.leases_released = Counter(
            "webhook_leases_released_total", "Leases released on shutdown", registry=r
        )
        self.inflight = Gauge("webhook_inflight_handlers", "Running handlers", registry=r)
        self.worker_slots = Gauge("webhook_worker_slots", "WORKER_CONCURRENCY", registry=r)
        self.db_write_lock = Histogram(
            "webhook_db_write_lock_seconds",
            "Write-lock hold time per transaction",
            ["op"],
            buckets=HTTP_BUCKETS,
            registry=r,
        )
        self.purged = Counter("webhook_purged_total", "Rows deleted by retention", registry=r)
        self.build_info = Gauge(
            "webhook_build_info", "Build information", ["version", "schema_version"], registry=r
        )


class QueueCollector(Collector):
    """Shared-DB gauges from the in-process queue-stats cache. Never queries the DB itself.

    Every replica reports the same values: aggregate with ``max()``, never ``sum()``.
    """

    def __init__(self, cache: object) -> None:
        self._cache = cache

    def describe(self) -> Iterator[GaugeMetricFamily]:
        yield GaugeMetricFamily("webhook_queue_events", "Events per status (shared DB)", labels=["status"])
        yield GaugeMetricFamily("webhook_oldest_queued_age_seconds", "Age of the oldest due queued event")
        yield GaugeMetricFamily("webhook_wal_bytes", "WAL file size")

    def collect(self) -> Iterator[GaugeMetricFamily]:
        stats = getattr(self._cache, "last", None)
        fresh = getattr(self._cache, "last_ok", False)
        queue = GaugeMetricFamily("webhook_queue_events", "Events per status (shared DB)", labels=["status"])
        if stats is not None and fresh:
            for status in ("queued", "processing", "failed"):
                queue.add_metric([status], getattr(stats, status))
        yield queue
        age = GaugeMetricFamily("webhook_oldest_queued_age_seconds", "Age of the oldest due queued event")
        wal = GaugeMetricFamily("webhook_wal_bytes", "WAL file size")
        if stats is not None:
            age.add_metric([], stats.oldest_queued_age_s)
            wal.add_metric([], stats.wal_bytes)
        yield age
        yield wal
