"""App factory. The lifespan runs migrations, then starts and stops the dispatcher and purger."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import IO

from fastapi import FastAPI

from webhook_receiver import __version__
from webhook_receiver.api import events, health
from webhook_receiver.api.body_limit import BodyLimitMiddleware
from webhook_receiver.api.problems import install_problem_handlers
from webhook_receiver.api.services import Services
from webhook_receiver.clock import Clock, SystemClock
from webhook_receiver.config import Settings
from webhook_receiver.fsguard import check_database_filesystem
from webhook_receiver.observability.logging import configure_logging, get_logger
from webhook_receiver.observability.metrics import Metrics, QueueCollector
from webhook_receiver.observability.queue_stats import QueueStatsCache
from webhook_receiver.observability.request_id import RequestIdMiddleware
from webhook_receiver.processing.handler import HandlerSpec
from webhook_receiver.processing.registry import HandlerRegistry, load_plugins
from webhook_receiver.processing.stub import StubHandler
from webhook_receiver.store.migrate import LATEST_SCHEMA_VERSION
from webhook_receiver.store.sqlite import SqliteEventStore
from webhook_receiver.worker.dispatcher import Dispatcher
from webhook_receiver.worker.loop_lag import LoopLagMonitor
from webhook_receiver.worker.purger import Purger

log = get_logger()


def build_registry(settings: Settings) -> HandlerRegistry:
    stub = StubHandler(
        min_seconds=settings.STUB_MIN_SECONDS,
        max_seconds=settings.STUB_MAX_SECONDS,
        failure_rate=settings.STUB_FAILURE_RATE,
        permanent_failure_rate=settings.STUB_PERMANENT_FAILURE_RATE,
    )
    spec = HandlerSpec(
        name="stub",
        timeout=settings.HANDLER_DEFAULT_TIMEOUT_SECONDS,
        max_concurrency=settings.WORKER_CONCURRENCY,
    )
    registry = HandlerRegistry(stub, spec)
    load_plugins(registry, settings.HANDLER_PLUGINS)
    return registry


def create_app(
    settings: Settings | None = None,
    *,
    clock: Clock | None = None,
    registry: HandlerRegistry | None = None,
    start_workers: bool = True,
    configure_logs: bool = True,
    log_stream: IO[str] | None = None,
) -> FastAPI:
    settings = settings or Settings()
    clock = clock or SystemClock()
    if configure_logs:
        configure_logging(settings.LOG_LEVEL, settings.LOG_FORMAT, settings.REPLICA_ID, log_stream)
    metrics = Metrics()
    registry = registry or build_registry(settings)
    store = SqliteEventStore(
        settings.DATABASE_PATH,
        busy_timeout_ms=settings.BUSY_TIMEOUT_MS,
        read_pool_size=settings.READ_POOL_SIZE,
        slow_write_ms=settings.DB_WRITE_SLOW_MS,
        on_write=lambda op, held: metrics.db_write_lock.labels(op).observe(held),
    )
    queue_stats = QueueStatsCache(store, clock, settings.HEALTH_CACHE_SECONDS)
    metrics.registry.register(QueueCollector(queue_stats))
    svc = Services(
        settings=settings,
        clock=clock,
        metrics=metrics,
        store=store,
        registry=registry,
        dispatcher=Dispatcher(store, registry, settings, metrics, clock),
        purger=Purger(store, settings, metrics, clock),
        lag_monitor=LoopLagMonitor(metrics, settings.LOOP_LAG_INTERVAL_SECONDS, settings.LOOP_LAG_WARN_MS),
        queue_stats=queue_stats,
        workers_enabled=start_workers,
    )

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        if settings.FS_GUARD_ENABLED:
            check_database_filesystem(settings.DATABASE_PATH)
        await store.open()
        metrics.build_info.labels(__version__, str(LATEST_SCHEMA_VERSION)).set(1)
        log.info(
            "startup",
            version=__version__,
            schema_version=store.schema_version_at_open,
            settings=settings.public_dump(),
        )
        if start_workers:
            await svc.dispatcher.start()
            await svc.purger.start()
            await svc.lag_monitor.start()
        try:
            yield
        finally:
            if start_workers:
                await svc.dispatcher.stop()
                await svc.purger.stop()
                await svc.lag_monitor.stop()
            await store.close()
            log.info("shutdown", version=__version__, schema_version=LATEST_SCHEMA_VERSION)

    app = FastAPI(title="Webhook Receiver", version=__version__, lifespan=lifespan)
    app.state.services = svc
    install_problem_handlers(app, metrics)
    app.include_router(events.router)
    app.include_router(health.router)
    if settings.METRICS_ENABLED:
        app.include_router(health.metrics_router)
    # Added last = outermost: the request id exists before the size check can reject anything.
    app.add_middleware(BodyLimitMiddleware, max_bytes=settings.MAX_BODY_BYTES, metrics=metrics)
    app.add_middleware(RequestIdMiddleware, metrics=metrics)
    return app
