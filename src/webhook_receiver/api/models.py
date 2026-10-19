"""Request and response models, and the canonical-JSON helpers used for dedup."""

from __future__ import annotations

import base64
import hashlib
import json
import uuid
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, JsonValue, StrictStr

from webhook_receiver.clock import iso_ms
from webhook_receiver.store.base import EventRecord

EventType = Annotated[StrictStr, Field(min_length=1, max_length=100, pattern=r"^[A-Za-z0-9._:-]+$")]
IdempotencyKey = Annotated[StrictStr, Field(min_length=1, max_length=255, pattern=r"^[\x20-\x7E]+$")]
Status = Literal["queued", "processing", "completed", "failed"]


class EventIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    idempotency_key: IdempotencyKey
    event_type: EventType
    payload: JsonValue


class Receipt(BaseModel):
    receipt_id: str
    status: str
    duplicate: bool
    received_at: str


class EventView(BaseModel):
    receipt_id: str
    event_type: str
    idempotency_key: str
    status: str
    attempts: int
    lease_expiries: int
    request_id: str
    received_at: str
    updated_at: str
    completed_at: str | None
    next_attempt_at: str
    result: JsonValue = None
    last_error: str | None
    failure_reason: str | None
    replay_count: int


class EventSummary(BaseModel):
    receipt_id: str
    event_type: str
    idempotency_key: str
    status: str
    attempts: int
    lease_expiries: int
    request_id: str
    received_at: str
    updated_at: str
    completed_at: str | None
    next_attempt_at: str
    last_error: str | None
    failure_reason: str | None
    replay_count: int


class EventPage(BaseModel):
    items: list[EventSummary]
    next_cursor: str | None


class NonFiniteNumberError(ValueError):
    pass


def _reject_constant(name: str) -> Any:
    raise NonFiniteNumberError(f"non-finite number {name} is not valid JSON")


def parse_json_strict(raw: bytes) -> Any:
    """``json.loads`` that rejects ``NaN`` / ``Infinity`` (Python accepts them by default)."""
    return json.loads(raw, parse_constant=_reject_constant)


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def request_hash(canonical: str) -> str:
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _summary_fields(record: EventRecord) -> dict[str, Any]:
    failed = record.status == "failed"
    return {
        "receipt_id": record.id,
        "event_type": record.event_type,
        "idempotency_key": record.idempotency_key,
        "status": record.status,
        "attempts": record.attempts,
        "lease_expiries": record.lease_expiries,
        "request_id": record.request_id,
        "received_at": iso_ms(record.received_at),
        "updated_at": iso_ms(record.updated_at),
        "completed_at": iso_ms(record.completed_at),
        "next_attempt_at": iso_ms(record.next_attempt_at),
        # last_error: when failed, or when queued again after a failed attempt (including a replay).
        "last_error": record.last_error if record.status in ("failed", "queued") else None,
        "failure_reason": record.failure_reason if failed else None,
        "replay_count": record.replay_count,
    }


def event_view(record: EventRecord) -> EventView:
    result = None
    if record.status == "completed" and record.result is not None:
        result = json.loads(record.result)
    return EventView(**_summary_fields(record), result=result)


def event_summary(record: EventRecord) -> EventSummary:
    return EventSummary(**_summary_fields(record))


def encode_cursor(last_id: str) -> str:
    return base64.urlsafe_b64encode(last_id.encode("ascii")).decode("ascii").rstrip("=")


def decode_cursor(cursor: str) -> str:
    """The id encoded in ``cursor``. Raises ``ValueError`` for anything that is not a cursor we issued."""
    padded = cursor + "=" * (-len(cursor) % 4)
    try:
        raw = base64.urlsafe_b64decode(padded.encode("ascii")).decode("ascii")
        parsed = uuid.UUID(raw)
    except (ValueError, UnicodeError) as exc:
        raise ValueError("invalid cursor") from exc
    if str(parsed) != raw:
        raise ValueError("invalid cursor")
    return raw
