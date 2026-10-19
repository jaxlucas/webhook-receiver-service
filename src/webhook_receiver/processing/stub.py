"""The stub handler: sleeps 2–5 s and returns a small receipt. Failure injection for tests and load tests."""

from __future__ import annotations

import asyncio
import hashlib
import json
import random
import time

from pydantic import JsonValue

from webhook_receiver.processing.errors import PermanentError
from webhook_receiver.processing.handler import Event, HandlerContext


class StubTransientError(Exception):
    pass


class StubHandler:
    def __init__(
        self,
        min_seconds: float = 2.0,
        max_seconds: float = 5.0,
        failure_rate: float = 0.0,
        permanent_failure_rate: float = 0.0,
        rng: random.Random | None = None,
    ) -> None:
        self.min_seconds = min_seconds
        self.max_seconds = max(max_seconds, min_seconds)
        self.failure_rate = failure_rate
        self.permanent_failure_rate = permanent_failure_rate
        self.rng = rng or random.Random()

    async def __call__(self, event: Event, ctx: HandlerContext) -> JsonValue:
        started = time.perf_counter()
        await asyncio.sleep(self.rng.uniform(self.min_seconds, self.max_seconds))
        if self.permanent_failure_rate and self.rng.random() < self.permanent_failure_rate:
            raise PermanentError("stub: injected permanent failure")
        if self.failure_rate and self.rng.random() < self.failure_rate:
            raise StubTransientError("stub: injected transient failure")
        canonical = json.dumps(event.payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        return {
            "handled_by": "stub",
            "event_type": event.event_type,
            "payload_sha256": hashlib.sha256(canonical.encode("utf-8")).hexdigest(),
            "duration_ms": round((time.perf_counter() - started) * 1000, 1),
        }
