"""Measures event-loop scheduling delay: a handler blocking the loop shows up before it causes double runs."""

from __future__ import annotations

import asyncio
import contextlib

from webhook_receiver.observability.logging import get_logger
from webhook_receiver.observability.metrics import Metrics

log = get_logger()


class LoopLagMonitor:
    def __init__(self, metrics: Metrics, interval: float = 1.0, warn_ms: float = 200.0) -> None:
        self.metrics = metrics
        self.interval = interval
        self.warn_ms = warn_ms
        self._task: asyncio.Task[None] | None = None

    async def _loop(self) -> None:
        loop = asyncio.get_running_loop()
        while True:
            expected = loop.time() + self.interval
            await asyncio.sleep(self.interval)
            lag = max(0.0, loop.time() - expected)
            self.metrics.loop_lag.observe(lag)
            if lag * 1000 > self.warn_ms:
                log.warning("event_loop_lag", lag_ms=round(lag * 1000, 1))

    async def start(self) -> None:
        self._task = asyncio.create_task(self._loop(), name="loop-lag")

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
