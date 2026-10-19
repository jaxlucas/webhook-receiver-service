"""Queue figures shared by ``/health`` and the Prometheus collector, cached in-process."""

from __future__ import annotations

import asyncio
import time

from webhook_receiver.clock import Clock
from webhook_receiver.store.base import EventStore, QueueStats


class QueueStatsCache:
    def __init__(self, store: EventStore, clock: Clock, ttl_seconds: float = 2.0) -> None:
        self.store = store
        self.clock = clock
        self.ttl = ttl_seconds
        self.last: QueueStats | None = None
        self.last_ok = False
        self.queries = 0
        self._fetched_at: float | None = None
        self._lock = asyncio.Lock()

    async def get(self) -> QueueStats:
        async with self._lock:
            fresh = self._fetched_at is not None and time.monotonic() - self._fetched_at < self.ttl
            if fresh and self.last_ok and self.last is not None:
                return self.last
            self.queries += 1
            self._fetched_at = time.monotonic()
            try:
                stats = await asyncio.wait_for(self.store.queue_stats(self.clock.now_ms()), timeout=1.0)
            except BaseException:
                self.last_ok = False
                raise
            self.last = stats
            self.last_ok = True
            return stats
