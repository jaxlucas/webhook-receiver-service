"""``/health`` (per-replica liveness and diagnostics) and ``/metrics`` (Prometheus text format)."""

from __future__ import annotations

import asyncio
from typing import Annotated, Any

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from webhook_receiver.api.services import Services, services
from webhook_receiver.observability.logging import get_logger, truncate_error
from webhook_receiver.store.migrate import LATEST_SCHEMA_VERSION

router = APIRouter()
metrics_router = APIRouter()
log = get_logger()
ServicesDep = Annotated[Services, Depends(services)]


@router.get("/health")
async def health(svc: ServicesDep) -> JSONResponse:
    db_ok = True
    schema_ok = True
    try:
        await asyncio.wait_for(svc.store.ping(), timeout=1.0)
        version = await asyncio.wait_for(svc.store.schema_version(), timeout=1.0)
        schema_ok = version == LATEST_SCHEMA_VERSION
    except Exception as exc:
        db_ok = False
        log.warning("health_db_failed", error=truncate_error(repr(exc)))
    dispatcher_ok = svc.dispatcher.alive() if svc.workers_enabled else True

    queue: dict[str, Any] | None = None
    if db_ok:
        try:
            stats = await svc.queue_stats.get()
            queue = {
                "queued": stats.queued,
                "processing": stats.processing,
                "failed": stats.failed,
                "oldest_queued_age_s": round(stats.oldest_queued_age_s, 3),
            }
        except Exception as exc:
            log.warning("health_queue_failed", error=truncate_error(repr(exc)))

    healthy = db_ok and schema_ok and dispatcher_ok
    body = {
        "status": "ok" if healthy else "error",
        "replica_id": svc.settings.REPLICA_ID,
        "db": "ok" if db_ok else "error",
        "schema": "ok" if schema_ok and db_ok else "error",
        "dispatcher": "ok" if dispatcher_ok else "error",
        "queue": queue,
        "wal_bytes": svc.store.wal_bytes(),
    }
    return JSONResponse(body, status_code=200 if healthy else 503)


@metrics_router.get("/metrics", include_in_schema=False)
async def metrics(svc: ServicesDep) -> Response:
    try:
        await svc.queue_stats.get()
    except Exception as exc:
        # Gauges keep their last value; webhook_queue_events gets no update for this scrape.
        log.warning("metrics_queue_failed", error=truncate_error(repr(exc)))
    return Response(generate_latest(svc.metrics.registry), media_type=CONTENT_TYPE_LATEST)
