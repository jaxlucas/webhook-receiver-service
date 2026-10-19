from __future__ import annotations

import asyncio
import io
import json
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx
import pytest
from fastapi import FastAPI

from webhook_receiver.clock import SystemClock
from webhook_receiver.config import Settings
from webhook_receiver.main import create_app
from webhook_receiver.observability.logging import configure_logging
from webhook_receiver.observability.metrics import Metrics
from webhook_receiver.processing.registry import HandlerRegistry
from webhook_receiver.store.sqlite import SqliteEventStore


class FakeClock:
    def __init__(self, start_ms: int | None = None) -> None:
        self.now = start_ms if start_ms is not None else SystemClock().now_ms()

    def now_ms(self) -> int:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += int(seconds * 1000)


class LogCapture:
    def __init__(self) -> None:
        self.stream = io.StringIO()

    def lines(self) -> list[dict[str, Any]]:
        out = []
        for raw in self.stream.getvalue().splitlines():
            raw = raw.strip()
            if raw:
                out.append(json.loads(raw))
        return out

    def events(self, name: str) -> list[dict[str, Any]]:
        return [line for line in self.lines() if line.get("event") == name]

    @property
    def text(self) -> str:
        return self.stream.getvalue()


def make_settings(tmp_path: Path, **overrides: Any) -> Settings:
    base: dict[str, Any] = {
        "DATABASE_PATH": str(tmp_path / "events.db"),
        "STUB_MIN_SECONDS": 0.0,
        "STUB_MAX_SECONDS": 0.005,
        "POLL_INTERVAL": 0.02,
        "BACKOFF_BASE_SECONDS": 0.01,
        "REAP_JITTER_SECONDS": 0.0,
        "FS_GUARD_ENABLED": False,
        "REPLICA_ID": "test-replica",
        "PURGE_INTERVAL_SECONDS": 3600,
        "LOG_LEVEL": "DEBUG",
        "SHUTDOWN_GRACE": 2.0,
    }
    base.update(overrides)
    return Settings(**base)


@dataclass
class Harness:
    app: FastAPI
    client: httpx.AsyncClient
    logs: LogCapture
    settings: Settings

    @property
    def services(self) -> Any:
        return self.app.state.services

    @property
    def store(self) -> SqliteEventStore:
        return self.services.store

    @property
    def metrics(self) -> Metrics:
        return self.services.metrics

    async def post_event(self, key: str = "k1", event_type: str = "order.paid", payload: Any = None,
                         **kwargs: Any) -> httpx.Response:
        body = {"idempotency_key": key, "event_type": event_type, "payload": payload}
        return await self.client.post("/events", json=body, **kwargs)

    async def wait_for_status(self, receipt_id: str, statuses: set[str], timeout: float = 5.0) -> dict:
        deadline = asyncio.get_running_loop().time() + timeout
        while True:
            response = await self.client.get(f"/events/{receipt_id}")
            body = response.json()
            if body.get("status") in statuses:
                return body
            if asyncio.get_running_loop().time() > deadline:
                raise AssertionError(f"event {receipt_id} stuck in {body.get('status')}: {body}")
            await asyncio.sleep(0.02)


AppFactory = Callable[..., Any]


@pytest.fixture
def logs() -> LogCapture:
    capture = LogCapture()
    configure_logging("DEBUG", "json", "test-replica", capture.stream)
    return capture


@pytest.fixture
def harness_factory(tmp_path: Path, logs: LogCapture) -> AppFactory:
    @asynccontextmanager
    async def factory(
        *,
        clock: Any = None,
        registry: HandlerRegistry | None = None,
        start_workers: bool = True,
        **overrides: Any,
    ) -> AsyncIterator[Harness]:
        settings = make_settings(tmp_path, **overrides)
        app = create_app(
            settings,
            clock=clock,
            registry=registry,
            start_workers=start_workers,
            log_stream=logs.stream,
        )
        async with app.router.lifespan_context(app):
            transport = httpx.ASGITransport(app=app, client=("10.0.0.1", 1234))
            async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
                yield Harness(app, client, logs, settings)

    return factory


@pytest.fixture
async def harness(harness_factory: AppFactory) -> AsyncIterator[Harness]:
    async with harness_factory() as h:
        yield h


@pytest.fixture
async def api(harness_factory: AppFactory) -> AsyncIterator[Harness]:
    """The API without the dispatcher, so events stay queued."""
    async with harness_factory(start_workers=False) as h:
        yield h


def metric_value(metrics: Metrics, name: str, labels: dict[str, str] | None = None) -> float:
    value = metrics.registry.get_sample_value(name, labels or {})
    return 0.0 if value is None else value
