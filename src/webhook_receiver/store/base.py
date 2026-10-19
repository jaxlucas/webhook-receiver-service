"""The ``EventStore`` protocol: the seam for a later Postgres implementation. All SQL lives behind it."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol

STATUSES = ("queued", "processing", "completed", "failed")
FAILURE_REASONS = ("permanent_error", "retries_exhausted", "timeout", "lease_expiries_exhausted")
MAX_LAST_ERROR_BYTES = 2048


class StoreUnavailableError(Exception):
    """The DB is locked beyond ``busy_timeout``, full, or erroring. Nothing was written."""

    def __init__(self, op: str, error: str) -> None:
        super().__init__(f"{op}: {error}")
        self.op = op
        self.error = error


class IngestOutcome(StrEnum):
    ACCEPTED = "accepted"
    DUPLICATE = "duplicate"
    CONFLICT = "conflict"
    REUSED_AFTER_TTL = "reused_after_ttl"


@dataclass(frozen=True, slots=True)
class EventRecord:
    id: str
    event_type: str
    idempotency_key: str
    request_hash: str
    status: str
    attempts: int
    lease_expiries: int
    next_attempt_at: int
    lease_owner: str | None
    lease_token: str | None
    lease_expires_at: int | None
    request_id: str
    result: str | None
    last_error: str | None
    failure_reason: str | None
    replay_count: int
    received_at: int
    updated_at: int
    completed_at: int | None


@dataclass(frozen=True, slots=True)
class NewEvent:
    id: str
    event_type: str
    idempotency_key: str
    request_hash: str
    payload: str
    request_id: str


@dataclass(frozen=True, slots=True)
class IngestResult:
    outcome: IngestOutcome
    record: EventRecord | None
    """The stored row for accepted/duplicate/reused; ``None`` for a conflict."""
    old_receipt_id: str | None = None
    original_request_id: str | None = None


@dataclass(frozen=True, slots=True)
class ClaimedEvent:
    id: str
    event_type: str
    idempotency_key: str
    payload: str
    attempts: int
    lease_expiries: int
    lease_token: str
    lease_expires_at: int
    request_id: str
    received_at: int
    due_at: int
    """``next_attempt_at`` at claim time: when the event became due (received or re-queued)."""


@dataclass(frozen=True, slots=True)
class OutcomeResult:
    """Row state after a fenced outcome write. ``None`` from the store means the fence matched 0 rows."""

    attempts: int
    lease_expiries: int
    status: str
    next_attempt_at: int


@dataclass(frozen=True, slots=True)
class ReapedLease:
    id: str
    event_type: str
    previous_owner: str | None
    lease_expiries: int
    attempts: int
    request_id: str
    outcome: str
    """``requeued`` or ``failed``."""


@dataclass(frozen=True, slots=True)
class ReplayResult:
    record: EventRecord | None
    """The replayed row, or ``None`` when the event is missing or not failed."""
    current_status: str | None
    """The status when the replay was rejected; ``None`` for unknown ids."""
    previous_failure_reason: str | None = None


@dataclass(frozen=True, slots=True)
class QueueStats:
    queued: int
    processing: int
    failed: int
    oldest_queued_age_s: float
    wal_bytes: int


@dataclass(frozen=True, slots=True)
class PurgeStats:
    deleted: int
    batches: int


class EventStore(Protocol):
    async def open(self) -> None: ...
    async def close(self) -> None: ...

    async def ingest(self, event: NewEvent, now: int, ttl_ms: int) -> IngestResult: ...
    async def get(self, event_id: str) -> EventRecord | None: ...
    async def list_events(
        self, status: str, event_type: str | None, before_id: str | None, limit: int
    ) -> list[EventRecord]: ...

    async def claim(
        self,
        replica: str,
        n: int,
        now: int,
        lease_ms: int,
        include_types: Sequence[str] | None = None,
        exclude_types: Sequence[str] | None = None,
    ) -> list[ClaimedEvent]: ...
    async def reap(self, now: int, max_lease_expiries: int, jitter_ms: int) -> list[ReapedLease]: ...
    async def complete(self, event_id: str, token: str, result: str, now: int) -> OutcomeResult | None: ...
    async def retry(
        self, event_id: str, token: str, error: str, next_attempt_at: int, now: int
    ) -> OutcomeResult | None: ...
    async def fail(
        self, event_id: str, token: str, error: str, reason: str, now: int
    ) -> OutcomeResult | None: ...
    async def release(self, replica: str, tokens: Sequence[str], now: int) -> int: ...
    async def renew(self, replica: str, tokens: Sequence[str], now: int, lease_ms: int) -> set[str]: ...
    async def replay(self, event_id: str, now: int) -> ReplayResult: ...
    async def purge(self, cutoff: int, batch: int, vacuum_pages: int) -> PurgeStats: ...

    async def ping(self) -> None: ...
    async def schema_version(self) -> int: ...
    async def queue_stats(self, now: int) -> QueueStats: ...


def truncate_last_error(text: str) -> str:
    encoded = text.encode("utf-8")
    if len(encoded) <= MAX_LAST_ERROR_BYTES:
        return text
    return encoded[: MAX_LAST_ERROR_BYTES - 3].decode("utf-8", errors="ignore") + "..."
