"""Per-app service container, reachable from routes as a dependency."""

from __future__ import annotations

from dataclasses import dataclass

from fastapi import Request

from webhook_receiver.clock import Clock
from webhook_receiver.config import Settings
from webhook_receiver.observability.metrics import Metrics
from webhook_receiver.observability.queue_stats import QueueStatsCache
from webhook_receiver.processing.registry import HandlerRegistry
from webhook_receiver.store.sqlite import SqliteEventStore
from webhook_receiver.worker.dispatcher import Dispatcher
from webhook_receiver.worker.loop_lag import LoopLagMonitor
from webhook_receiver.worker.purger import Purger


@dataclass(slots=True)
class Services:
    settings: Settings
    clock: Clock
    metrics: Metrics
    store: SqliteEventStore
    registry: HandlerRegistry
    dispatcher: Dispatcher
    purger: Purger
    lag_monitor: LoopLagMonitor
    queue_stats: QueueStatsCache
    workers_enabled: bool = True

    def wake_dispatcher(self) -> None:
        self.dispatcher.wake()


def services(request: Request) -> Services:
    return request.app.state.services
