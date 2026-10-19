"""Retention: delete events that finished more than ``IDEMPOTENCY_TTL_SECONDS`` ago."""

from __future__ import annotations

import asyncio
import contextlib
import time

from webhook_receiver.clock import Clock
from webhook_receiver.config import Settings
from webhook_receiver.observability.logging import get_logger, truncate_error
from webhook_receiver.observability.metrics import Metrics
from webhook_receiver.store.base import EventStore, PurgeStats

log = get_logger()


class Purger:
    def __init__(self, store: EventStore, settings: Settings, metrics: Metrics, clock: Clock) -> None:
        self.store = store
        self.settings = settings
        self.metrics = metrics
        self.clock = clock
        self._task: asyncio.Task[None] | None = None

    async def run_once(self) -> PurgeStats:
        started = time.perf_counter()
        cutoff = self.clock.now_ms() - int(self.settings.IDEMPOTENCY_TTL_SECONDS * 1000)
        stats = await self.store.purge(cutoff, self.settings.PURGE_BATCH, self.settings.PURGE_VACUUM_PAGES)
        self.metrics.purged.inc(stats.deleted)
        log.info(
            "purge_run",
            deleted=stats.deleted,
            batches=stats.batches,
            duration_ms=round((time.perf_counter() - started) * 1000, 1),
        )
        return stats

    async def _loop(self) -> None:
        while True:
            await asyncio.sleep(self.settings.PURGE_INTERVAL_SECONDS)
            try:
                await self.run_once()
            except Exception as exc:
                log.error("purge_failed", error=truncate_error(repr(exc)))

    async def start(self) -> None:
        self._task = asyncio.create_task(self._loop(), name="purger")

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
