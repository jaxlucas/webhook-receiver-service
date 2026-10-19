"""``/events`` routes: ingest with dedup, status lookup, listing, and manual replay."""

from __future__ import annotations

import uuid
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import ValidationError

from webhook_receiver.api.auth import authenticate
from webhook_receiver.api.models import (
    EventIn,
    EventPage,
    EventView,
    NonFiniteNumberError,
    Receipt,
    Status,
    canonical_json,
    decode_cursor,
    encode_cursor,
    event_summary,
    event_view,
    parse_json_strict,
    request_hash,
)
from webhook_receiver.api.problems import problem
from webhook_receiver.api.services import Services, services
from webhook_receiver.clock import iso_ms
from webhook_receiver.observability.logging import get_logger
from webhook_receiver.observability.request_id import current_request_id, new_request_id
from webhook_receiver.store.base import IngestOutcome, NewEvent

router = APIRouter(dependencies=[Depends(authenticate)])
log = get_logger()

ServicesDep = Annotated[Services, Depends(services)]

POST_BODY_SCHEMA = {
    "requestBody": {
        "required": True,
        "content": {"application/json": {"schema": EventIn.model_json_schema()}},
    }
}


def _is_json_content_type(value: str | None) -> bool:
    if not value:
        return False
    media_type = value.split(";", 1)[0].strip().lower()
    return media_type == "application/json" or (
        media_type.startswith("application/") and media_type.endswith("+json")
    )


def _parse_event(raw: bytes, content_type: str | None) -> EventIn:
    if not _is_json_content_type(content_type):
        raise RequestValidationError(
            [{"loc": ("header", "content-type"), "msg": "Content-Type must be application/json"}]
        )
    try:
        data = parse_json_strict(raw)
    except NonFiniteNumberError as exc:
        raise RequestValidationError([{"loc": ("body",), "msg": str(exc)}]) from exc
    except (ValueError, UnicodeDecodeError) as exc:
        raise RequestValidationError([{"loc": ("body",), "msg": f"Invalid JSON: {exc}"}]) from exc
    if not isinstance(data, dict):
        raise RequestValidationError([{"loc": ("body",), "msg": "The body must be a JSON object"}])
    try:
        return EventIn.model_validate(data)
    except ValidationError as exc:
        errors = [{"loc": ("body", *err["loc"]), "msg": err["msg"]} for err in exc.errors()]
        raise RequestValidationError(errors) from exc


@router.post(
    "/events",
    status_code=202,
    response_model=Receipt,
    openapi_extra=POST_BODY_SCHEMA,
    responses={200: {"model": Receipt, "description": "Duplicate of an existing event"}},
)
async def post_event(request: Request, svc: ServicesDep) -> JSONResponse:
    event = _parse_event(await request.body(), request.headers.get("content-type"))
    # All CPU work happens before the write transaction begins.
    canonical = canonical_json(event.payload)
    request_id = current_request_id() or new_request_id()
    new = NewEvent(
        id=str(uuid.uuid7()),
        event_type=event.event_type,
        idempotency_key=event.idempotency_key,
        request_hash=request_hash(canonical),
        payload=canonical,
        request_id=request_id,
    )
    ttl_ms = int(svc.settings.IDEMPOTENCY_TTL_SECONDS * 1000)
    result = await svc.store.ingest(new, svc.clock.now_ms(), ttl_ms)
    svc.metrics.ingest.labels(result.outcome.value).inc()
    ids = {"event_type": event.event_type, "idempotency_key": event.idempotency_key}

    if result.outcome is IngestOutcome.CONFLICT:
        log.warning("event_key_conflict", **ids)
        return problem(
            409,
            "idempotency-key-reused",
            "Idempotency Key Reused",
            "This idempotency key was already used for this event_type with a different payload.",
        )

    record = result.record
    assert record is not None
    receipt = Receipt(
        receipt_id=record.id,
        status=record.status,
        duplicate=result.outcome is IngestOutcome.DUPLICATE,
        received_at=iso_ms(record.received_at) or "",
    )
    if result.outcome is IngestOutcome.DUPLICATE:
        log.info(
            "event_duplicate", receipt_id=record.id, original_request_id=result.original_request_id, **ids
        )
        return JSONResponse(receipt.model_dump(), status_code=200)

    if result.outcome is IngestOutcome.REUSED_AFTER_TTL:
        log.info("event_key_reused_after_ttl", receipt_id=record.id, old_receipt_id=result.old_receipt_id)
    else:
        log.info("event_accepted", receipt_id=record.id, **ids)
    svc.wake_dispatcher()
    return JSONResponse(receipt.model_dump(), status_code=202, headers={"Location": f"/events/{record.id}"})


@router.get("/events", response_model=EventPage)
async def list_events(
    svc: ServicesDep,
    status: Annotated[Status, Query()],
    event_type: Annotated[str | None, Query(min_length=1, max_length=100)] = None,
    limit: Annotated[int, Query(ge=1, le=100)] = 50,
    cursor: Annotated[str | None, Query(max_length=200)] = None,
) -> EventPage:
    before_id = None
    if cursor is not None:
        try:
            before_id = decode_cursor(cursor)
        except ValueError as exc:
            raise RequestValidationError([{"loc": ("query", "cursor"), "msg": "Invalid cursor"}]) from exc
    records = await svc.store.list_events(status, event_type, before_id, limit + 1)
    has_more = len(records) > limit
    records = records[:limit]
    next_cursor = encode_cursor(records[-1].id) if has_more and records else None
    return EventPage(items=[event_summary(r) for r in records], next_cursor=next_cursor)


@router.get("/events/{receipt_id}", response_model=EventView)
async def get_event(receipt_id: uuid.UUID, svc: ServicesDep) -> Any:
    record = await svc.store.get(str(receipt_id))
    if record is None:
        return problem(
            404, "not-found", "Not Found", "No event with this receipt id (it may have been purged)."
        )
    return event_view(record)


@router.post("/events/{receipt_id}/retry", status_code=202, response_model=EventView)
async def retry_event(receipt_id: uuid.UUID, svc: ServicesDep) -> Any:
    event_id = str(receipt_id)
    result = await svc.store.replay(event_id, svc.clock.now_ms())
    if result.record is None and result.current_status is None:
        svc.metrics.replays.labels("not_found").inc()
        return problem(
            404, "not-found", "Not Found", "No event with this receipt id (it may have been purged)."
        )
    if result.record is None:
        svc.metrics.replays.labels("not_failed").inc()
        log.info("event_replay_rejected", receipt_id=event_id, current_status=result.current_status)
        return problem(
            409,
            "event-not-failed",
            "Event Not Failed",
            "Only failed events can be replayed.",
            extra={"status": result.current_status},
        )
    svc.metrics.replays.labels("accepted").inc()
    log.info(
        "event_replayed",
        receipt_id=event_id,
        previous_failure_reason=result.previous_failure_reason,
        replay_count=result.record.replay_count,
    )
    svc.wake_dispatcher()
    view = event_view(result.record)
    return JSONResponse(
        view.model_dump(mode="json"), status_code=202, headers={"Location": f"/events/{event_id}"}
    )
