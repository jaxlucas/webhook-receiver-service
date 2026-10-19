"""SQLite implementation of ``EventStore``.

Access rules (see the spec's Architecture section):

- one ``aiosqlite`` write connection per replica, autocommit, serialised in-process by an ``asyncio.Lock``;
- every write is ``BEGIN IMMEDIATE`` → a few statements → ``COMMIT``, with all CPU work done before ``BEGIN``;
- a small pool of read connections;
- ``journal_mode=WAL``, ``synchronous=FULL``, ``busy_timeout``.
"""

from __future__ import annotations

import asyncio
import os
import sqlite3
import time
from collections.abc import Awaitable, Callable, Sequence
from typing import Any

import aiosqlite

from webhook_receiver.observability.logging import get_logger
from webhook_receiver.store.base import (
    ClaimedEvent,
    EventRecord,
    IngestOutcome,
    IngestResult,
    NewEvent,
    OutcomeResult,
    PurgeStats,
    QueueStats,
    ReapedLease,
    ReplayResult,
    StoreUnavailableError,
    truncate_last_error,
)
from webhook_receiver.store.migrate import migrate

log = get_logger()

RECORD_COLUMNS = (
    "id, event_type, idempotency_key, request_hash, status, attempts, lease_expiries, next_attempt_at, "
    "lease_owner, lease_token, lease_expires_at, request_id, result, last_error, failure_reason, "
    "replay_count, received_at, updated_at, completed_at"
)
FINISHED = "('completed', 'failed')"
# SQLite's default limit on bound parameters is 32766; keep chunks far below it.
TOKEN_CHUNK = 500

UNAVAILABLE_CODES = {
    sqlite3.SQLITE_BUSY,
    sqlite3.SQLITE_LOCKED,
    sqlite3.SQLITE_FULL,
    sqlite3.SQLITE_IOERR,
    sqlite3.SQLITE_CANTOPEN,
    sqlite3.SQLITE_READONLY,
}

WriteObserver = Callable[[str, float], None]


def _record(row: Sequence[Any]) -> EventRecord:
    return EventRecord(*row)


def _is_unavailable(exc: sqlite3.Error) -> bool:
    code = getattr(exc, "sqlite_errorcode", None)
    if code is not None and (code & 0xFF) in UNAVAILABLE_CODES:
        return True
    text = str(exc).lower()
    return "locked" in text or "busy" in text or "disk" in text


def _chunks(items: Sequence[str], size: int = TOKEN_CHUNK) -> list[Sequence[str]]:
    return [items[i : i + size] for i in range(0, len(items), size)]


def _placeholders(n: int) -> str:
    return ",".join("?" * n)


class SqliteEventStore:
    def __init__(
        self,
        path: str,
        *,
        busy_timeout_ms: int = 5000,
        read_pool_size: int = 4,
        slow_write_ms: float = 50.0,
        on_write: WriteObserver | None = None,
    ) -> None:
        self.path = path
        self.busy_timeout_ms = busy_timeout_ms
        self.read_pool_size = read_pool_size
        self.slow_write_ms = slow_write_ms
        self.on_write = on_write
        self._writer: aiosqlite.Connection | None = None
        self._write_lock = asyncio.Lock()
        self._readers: asyncio.Queue[aiosqlite.Connection] = asyncio.Queue()
        self._all_readers: list[aiosqlite.Connection] = []
        self.schema_version_at_open = 0

    # ---------------------------------------------------------------- lifecycle

    async def open(self) -> None:
        directory = os.path.dirname(os.path.abspath(self.path))
        os.makedirs(directory, exist_ok=True)
        self.schema_version_at_open = await asyncio.to_thread(migrate, self.path, self.busy_timeout_ms)
        self._writer = await self._connect()
        for _ in range(self.read_pool_size):
            reader = await self._connect()
            self._all_readers.append(reader)
            self._readers.put_nowait(reader)

    async def _connect(self) -> aiosqlite.Connection:
        conn = await aiosqlite.connect(self.path, isolation_level=None, timeout=self.busy_timeout_ms / 1000)
        await conn.execute(f"PRAGMA busy_timeout={int(self.busy_timeout_ms)}")
        await conn.execute("PRAGMA journal_mode=WAL")
        await conn.execute("PRAGMA synchronous=FULL")
        return conn

    async def close(self) -> None:
        if self._writer is not None:
            await self._writer.close()
            self._writer = None
        for reader in self._all_readers:
            await reader.close()
        self._all_readers.clear()
        self._readers = asyncio.Queue()

    @property
    def writer(self) -> aiosqlite.Connection:
        if self._writer is None:
            raise RuntimeError("store is not open")
        return self._writer

    # ---------------------------------------------------------------- plumbing

    async def _write[T](self, op: str, body: Callable[[aiosqlite.Connection], Awaitable[T]]) -> T:
        # Shielded: a cancelled caller must never leave the shared write connection mid-transaction.
        return await asyncio.shield(self._write_txn(op, body))

    async def _write_txn[T](self, op: str, body: Callable[[aiosqlite.Connection], Awaitable[T]]) -> T:
        async with self._write_lock:
            conn = self.writer
            begun_at: float | None = None
            try:
                await conn.execute("BEGIN IMMEDIATE")
                begun_at = time.perf_counter()
                value = await body(conn)
                await conn.execute("COMMIT")
            except sqlite3.Error as exc:
                if conn.in_transaction:
                    try:
                        await conn.execute("ROLLBACK")
                    except sqlite3.Error:
                        pass
                if _is_unavailable(exc):
                    raise StoreUnavailableError(op, str(exc)) from exc
                raise
            except BaseException:
                if conn.in_transaction:
                    await conn.execute("ROLLBACK")
                raise
            finally:
                if begun_at is not None:
                    self._observe_write(op, time.perf_counter() - begun_at)
            return value

    def _observe_write(self, op: str, held_s: float) -> None:
        if self.on_write is not None:
            self.on_write(op, held_s)
        held_ms = held_s * 1000
        if held_ms > self.slow_write_ms:
            log.warning("db_write_slow", op=op, lock_held_ms=round(held_ms, 1))

    async def _read[T](self, op: str, body: Callable[[aiosqlite.Connection], Awaitable[T]]) -> T:
        conn = await self._readers.get()
        try:
            return await body(conn)
        except sqlite3.Error as exc:
            if _is_unavailable(exc):
                raise StoreUnavailableError(op, str(exc)) from exc
            raise
        finally:
            self._readers.put_nowait(conn)

    @staticmethod
    async def _fetchall(conn: aiosqlite.Connection, sql: str, params: Sequence[Any] = ()) -> list[Any]:
        async with conn.execute(sql, params) as cursor:
            return list(await cursor.fetchall())

    @staticmethod
    async def _fetchone(conn: aiosqlite.Connection, sql: str, params: Sequence[Any] = ()) -> Any:
        async with conn.execute(sql, params) as cursor:
            return await cursor.fetchone()

    # ---------------------------------------------------------------- ingest

    async def ingest(self, event: NewEvent, now: int, ttl_ms: int) -> IngestResult:
        cutoff = now - ttl_ms
        insert_sql = (
            "INSERT INTO events (id, event_type, idempotency_key, request_hash, payload, status, "
            "next_attempt_at, request_id, received_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, 'queued', ?, ?, ?, ?) "
            f"RETURNING {RECORD_COLUMNS}"
        )
        insert_params = (
            event.id,
            event.event_type,
            event.idempotency_key,
            event.request_hash,
            event.payload,
            now,
            event.request_id,
            now,
            now,
        )

        async def body(conn: aiosqlite.Connection) -> IngestResult:
            existing = await self._fetchone(
                conn,
                f"SELECT {RECORD_COLUMNS} FROM events WHERE event_type = ? AND idempotency_key = ?",
                (event.event_type, event.idempotency_key),
            )
            if existing is None:
                rows = await self._fetchall(conn, insert_sql, insert_params)
                return IngestResult(IngestOutcome.ACCEPTED, _record(rows[0]))
            old = _record(existing)
            # Expiry is checked before the payload comparison: an expired key is free, whatever the body.
            if (
                old.status in ("completed", "failed")
                and old.completed_at is not None
                and (old.completed_at <= cutoff)
            ):
                await conn.execute("DELETE FROM events WHERE id = ?", (old.id,))
                rows = await self._fetchall(conn, insert_sql, insert_params)
                return IngestResult(IngestOutcome.REUSED_AFTER_TTL, _record(rows[0]), old_receipt_id=old.id)
            if old.request_hash != event.request_hash:
                return IngestResult(IngestOutcome.CONFLICT, None)
            return IngestResult(IngestOutcome.DUPLICATE, old, original_request_id=old.request_id)

        return await self._write("insert", body)

    # ---------------------------------------------------------------- reads

    async def get(self, event_id: str) -> EventRecord | None:
        async def body(conn: aiosqlite.Connection) -> EventRecord | None:
            row = await self._fetchone(conn, f"SELECT {RECORD_COLUMNS} FROM events WHERE id = ?", (event_id,))
            return None if row is None else _record(row)

        return await self._read("get", body)

    async def get_payload(self, event_id: str) -> str | None:
        async def body(conn: aiosqlite.Connection) -> str | None:
            row = await self._fetchone(conn, "SELECT payload FROM events WHERE id = ?", (event_id,))
            return None if row is None else row[0]

        return await self._read("get", body)

    @staticmethod
    def list_sql(event_type: str | None, before_id: str | None) -> tuple[str, str]:
        index = "ix_events_status_type_id" if event_type is not None else "ix_events_status_id"
        where = ["status = ?"]
        if event_type is not None:
            where.append("event_type = ?")
        if before_id is not None:
            where.append("id < ?")
        sql = (
            f"SELECT {RECORD_COLUMNS} FROM events INDEXED BY {index} "
            f"WHERE {' AND '.join(where)} ORDER BY id DESC LIMIT ?"
        )
        return sql, index

    async def list_events(
        self, status: str, event_type: str | None, before_id: str | None, limit: int
    ) -> list[EventRecord]:
        sql, _ = self.list_sql(event_type, before_id)
        params: list[Any] = [status]
        if event_type is not None:
            params.append(event_type)
        if before_id is not None:
            params.append(before_id)
        params.append(limit)

        async def body(conn: aiosqlite.Connection) -> list[EventRecord]:
            return [_record(row) for row in await self._fetchall(conn, sql, params)]

        return await self._read("list", body)

    # ---------------------------------------------------------------- processing

    @staticmethod
    def claim_sql(n_include: int | None, n_exclude: int | None) -> str:
        filters = ""
        if n_include is not None:
            filters = f" AND event_type IN ({_placeholders(n_include)})"
        elif n_exclude:
            filters = f" AND event_type NOT IN ({_placeholders(n_exclude)})"
        return (
            "UPDATE events SET status = 'processing', lease_owner = ?, "
            "lease_token = lower(hex(randomblob(16))), lease_expires_at = ?, updated_at = ? "
            "WHERE id IN (SELECT id FROM events WHERE status = 'queued' AND next_attempt_at <= ?"
            f"{filters} ORDER BY next_attempt_at LIMIT ?) "
            "RETURNING id, event_type, idempotency_key, payload, attempts, lease_expiries, lease_token, "
            "lease_expires_at, request_id, received_at, next_attempt_at"
        )

    async def claim(
        self,
        replica: str,
        n: int,
        now: int,
        lease_ms: int,
        include_types: Sequence[str] | None = None,
        exclude_types: Sequence[str] | None = None,
    ) -> list[ClaimedEvent]:
        if n <= 0 or (include_types is not None and not include_types):
            return []
        include = list(include_types) if include_types is not None else None
        exclude = list(exclude_types) if exclude_types else None
        sql = self.claim_sql(len(include) if include is not None else None, len(exclude) if exclude else None)
        params: list[Any] = [replica, now + lease_ms, now, now]
        params.extend(include or exclude or [])
        params.append(n)

        async def body(conn: aiosqlite.Connection) -> list[ClaimedEvent]:
            return [ClaimedEvent(*row) for row in await self._fetchall(conn, sql, params)]

        return await self._write("claim", body)

    async def reap(self, now: int, max_lease_expiries: int, jitter_ms: int) -> list[ReapedLease]:
        jitter = max(1, int(jitter_ms))

        async def body(conn: aiosqlite.Connection) -> list[ReapedLease]:
            owners = await self._fetchall(
                conn,
                "SELECT id, lease_owner FROM events WHERE status = 'processing' AND lease_expires_at <= ?",
                (now,),
            )
            if not owners:
                return []
            rows = await self._fetchall(
                conn,
                """
                UPDATE events
                SET status = CASE WHEN lease_expiries + 1 >= :max THEN 'failed' ELSE 'queued' END,
                    last_error = CASE WHEN lease_expiries + 1 >= :max
                                      THEN 'lease expired too many times' ELSE last_error END,
                    failure_reason = CASE WHEN lease_expiries + 1 >= :max
                                          THEN 'lease_expiries_exhausted' ELSE NULL END,
                    completed_at = CASE WHEN lease_expiries + 1 >= :max THEN :now ELSE NULL END,
                    lease_expiries = lease_expiries + 1, lease_owner = NULL, lease_token = NULL,
                    lease_expires_at = NULL,
                    next_attempt_at = :now + (abs(random()) % :jitter), updated_at = :now
                WHERE status = 'processing' AND lease_expires_at <= :now
                RETURNING id, status, lease_expiries, event_type, attempts, request_id
                """,
                {"max": max_lease_expiries, "now": now, "jitter": jitter},
            )
            previous = dict(owners)
            return [
                ReapedLease(
                    id=row[0],
                    event_type=row[3],
                    previous_owner=previous.get(row[0]),
                    lease_expiries=row[2],
                    attempts=row[4],
                    request_id=row[5],
                    outcome="failed" if row[1] == "failed" else "requeued",
                )
                for row in rows
            ]

        return await self._write("reap", body)

    async def _outcome(self, sql: str, params: Sequence[Any]) -> OutcomeResult | None:
        async def body(conn: aiosqlite.Connection) -> OutcomeResult | None:
            rows = await self._fetchall(conn, sql, params)
            return OutcomeResult(*rows[0]) if rows else None

        return await self._write("outcome", body)

    async def complete(self, event_id: str, token: str, result: str, now: int) -> OutcomeResult | None:
        return await self._outcome(
            "UPDATE events SET status = 'completed', result = ?, completed_at = ?, attempts = attempts + 1, "
            "lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL, updated_at = ? "
            "WHERE id = ? AND lease_token = ? "
            "RETURNING attempts, lease_expiries, status, next_attempt_at",
            (result, now, now, event_id, token),
        )

    async def retry(
        self, event_id: str, token: str, error: str, next_attempt_at: int, now: int
    ) -> OutcomeResult | None:
        return await self._outcome(
            "UPDATE events SET status = 'queued', attempts = attempts + 1, next_attempt_at = ?, "
            "last_error = ?, lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL, updated_at = ? "
            "WHERE id = ? AND lease_token = ? "
            "RETURNING attempts, lease_expiries, status, next_attempt_at",
            (next_attempt_at, truncate_last_error(error), now, event_id, token),
        )

    async def fail(
        self, event_id: str, token: str, error: str, reason: str, now: int
    ) -> OutcomeResult | None:
        return await self._outcome(
            "UPDATE events SET status = 'failed', attempts = attempts + 1, last_error = ?, "
            "failure_reason = ?, completed_at = ?, lease_owner = NULL, lease_token = NULL, "
            "lease_expires_at = NULL, updated_at = ? "
            "WHERE id = ? AND lease_token = ? "
            "RETURNING attempts, lease_expiries, status, next_attempt_at",
            (truncate_last_error(error), reason, now, now, event_id, token),
        )

    async def release(self, replica: str, tokens: Sequence[str], now: int) -> int:
        tokens = list(tokens)
        if not tokens:
            return 0

        async def body(conn: aiosqlite.Connection) -> int:
            released = 0
            for chunk in _chunks(tokens):
                rows = await self._fetchall(
                    conn,
                    "UPDATE events SET status = 'queued', lease_owner = NULL, lease_token = NULL, "
                    "lease_expires_at = NULL, next_attempt_at = ?, updated_at = ? "
                    f"WHERE status = 'processing' AND lease_owner = ? AND lease_token IN "
                    f"({_placeholders(len(chunk))}) RETURNING id",
                    [now, now, replica, *chunk],
                )
                released += len(rows)
            return released

        return await self._write("release", body)

    async def renew(self, replica: str, tokens: Sequence[str], now: int, lease_ms: int) -> set[str]:
        tokens = list(tokens)
        if not tokens:
            return set()

        async def body(conn: aiosqlite.Connection) -> set[str]:
            renewed: set[str] = set()
            for chunk in _chunks(tokens):
                rows = await self._fetchall(
                    conn,
                    "UPDATE events SET lease_expires_at = ?, updated_at = ? "
                    "WHERE status = 'processing' AND lease_owner = ? AND lease_token IN "
                    f"({_placeholders(len(chunk))}) RETURNING lease_token",
                    [now + lease_ms, now, replica, *chunk],
                )
                renewed.update(row[0] for row in rows)
            return renewed

        return await self._write("renew", body)

    async def replay(self, event_id: str, now: int) -> ReplayResult:
        async def body(conn: aiosqlite.Connection) -> ReplayResult:
            current = await self._fetchone(
                conn, "SELECT status, failure_reason FROM events WHERE id = ?", (event_id,)
            )
            if current is None:
                return ReplayResult(None, None)
            if current[0] != "failed":
                return ReplayResult(None, current[0])
            rows = await self._fetchall(
                conn,
                "UPDATE events SET status = 'queued', attempts = 0, lease_expiries = 0, "
                "failure_reason = NULL, completed_at = NULL, next_attempt_at = ?, "
                "replay_count = replay_count + 1, updated_at = ? "
                f"WHERE id = ? AND status = 'failed' RETURNING {RECORD_COLUMNS}",
                (now, now, event_id),
            )
            return ReplayResult(_record(rows[0]), "queued", previous_failure_reason=current[1])

        return await self._write("outcome", body)

    # ---------------------------------------------------------------- retention

    PURGE_SQL = (
        "DELETE FROM events WHERE id IN (SELECT id FROM events INDEXED BY ix_events_purge "
        f"WHERE status IN {FINISHED} AND completed_at <= ? LIMIT ?)"
    )

    async def purge(self, cutoff: int, batch: int, vacuum_pages: int) -> PurgeStats:
        deleted = 0
        batches = 0
        while True:

            async def body(conn: aiosqlite.Connection) -> int:
                cursor = await conn.execute(self.PURGE_SQL, (cutoff, batch))
                count = cursor.rowcount
                await cursor.close()
                return count

            count = await self._write("purge", body)
            batches += 1
            deleted += count
            if count < batch:
                break
            await asyncio.sleep(0)
        await self.maintenance(vacuum_pages)
        return PurgeStats(deleted=deleted, batches=batches)

    async def maintenance(self, vacuum_pages: int) -> None:
        """Reclaim freed pages and truncate the WAL. Outside a transaction, under the in-process lock."""
        async with self._write_lock:
            try:
                if vacuum_pages > 0:
                    cursor = await self.writer.execute(f"PRAGMA incremental_vacuum({int(vacuum_pages)})")
                    await cursor.fetchall()
                    await cursor.close()
                cursor = await self.writer.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                await cursor.fetchall()
                await cursor.close()
            except sqlite3.Error as exc:
                log.warning("db_maintenance_failed", error=str(exc))

    # ---------------------------------------------------------------- health

    async def ping(self) -> None:
        async def body(conn: aiosqlite.Connection) -> None:
            await self._fetchone(conn, "SELECT 1")

        await self._read("ping", body)

    async def schema_version(self) -> int:
        async def body(conn: aiosqlite.Connection) -> int:
            row = await self._fetchone(conn, "PRAGMA user_version")
            return int(row[0])

        return await self._read("schema_version", body)

    async def queue_stats(self, now: int) -> QueueStats:
        async def body(conn: aiosqlite.Connection) -> QueueStats:
            counts = {}
            for status in ("queued", "processing", "failed"):
                row = await self._fetchone(
                    conn,
                    "SELECT count(*) FROM events INDEXED BY ix_events_status_id WHERE status = ?",
                    (status,),
                )
                counts[status] = int(row[0])
            oldest = await self._fetchone(
                conn,
                "SELECT min(next_attempt_at) FROM events WHERE status = 'queued' AND next_attempt_at <= ?",
                (now,),
            )
            age = 0.0 if oldest[0] is None else max(0.0, (now - int(oldest[0])) / 1000)
            return QueueStats(
                queued=counts["queued"],
                processing=counts["processing"],
                failed=counts["failed"],
                oldest_queued_age_s=age,
                wal_bytes=self.wal_bytes(),
            )

        return await self._read("queue_stats", body)

    def wal_bytes(self) -> int:
        try:
            return os.path.getsize(self.path + "-wal")
        except OSError:
            return 0
