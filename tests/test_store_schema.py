"""Phase 1 step 3: migrations, pragmas, indexes, write-lock instrumentation."""

from __future__ import annotations

import asyncio
import multiprocessing
import sqlite3
from pathlib import Path

import pytest

from tests.conftest import LogCapture
from webhook_receiver.store.migrate import LATEST_SCHEMA_VERSION, SchemaTooNewError, migrate
from webhook_receiver.store.sqlite import SqliteEventStore


def _plan(db: Path, sql: str, params: tuple = ()) -> str:
    conn = sqlite3.connect(db)
    try:
        rows = conn.execute(f"EXPLAIN QUERY PLAN {sql}", params).fetchall()
    finally:
        conn.close()
    return "\n".join(row[3] for row in rows)


def _assert_indexed(plan: str) -> None:
    assert "SCAN events" not in plan, plan
    assert "USE TEMP B-TREE" not in plan, plan


def test_migration_is_idempotent(tmp_path: Path) -> None:
    db = tmp_path / "e.db"
    assert migrate(db) == LATEST_SCHEMA_VERSION
    assert migrate(db) == LATEST_SCHEMA_VERSION
    conn = sqlite3.connect(db)
    assert conn.execute("PRAGMA user_version").fetchone()[0] == LATEST_SCHEMA_VERSION
    tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    assert tables == [("events",)]


def _migrate_in_process(path: str, barrier: multiprocessing.synchronize.Barrier) -> int:
    barrier.wait()
    return migrate(path)


def test_concurrent_processes_apply_migration_once(tmp_path: Path) -> None:
    db = str(tmp_path / "e.db")
    ctx = multiprocessing.get_context("spawn")
    with ctx.Manager() as manager:
        barrier = manager.Barrier(4)
        with ctx.Pool(4) as pool:
            results = pool.starmap(_migrate_in_process, [(db, barrier)] * 4)
    assert results == [LATEST_SCHEMA_VERSION] * 4
    conn = sqlite3.connect(db)
    assert conn.execute("PRAGMA user_version").fetchone()[0] == LATEST_SCHEMA_VERSION
    indexes = conn.execute("SELECT count(*) FROM sqlite_master WHERE type='index' AND name NOT LIKE 'sqlite_%'")
    assert indexes.fetchone()[0] == 6


def test_newer_schema_refuses_to_start(tmp_path: Path) -> None:
    db = tmp_path / "e.db"
    migrate(db)
    conn = sqlite3.connect(db)
    conn.execute(f"PRAGMA user_version={LATEST_SCHEMA_VERSION + 1}")
    conn.commit()
    conn.close()
    with pytest.raises(SchemaTooNewError):
        migrate(db)


def test_fresh_file_uses_incremental_auto_vacuum_and_wal(tmp_path: Path) -> None:
    db = tmp_path / "e.db"
    migrate(db)
    conn = sqlite3.connect(db)
    assert conn.execute("PRAGMA auto_vacuum").fetchone()[0] == 2
    assert conn.execute("PRAGMA journal_mode").fetchone()[0] == "wal"


async def test_store_connections_use_required_pragmas(tmp_path: Path) -> None:
    store = SqliteEventStore(str(tmp_path / "e.db"), busy_timeout_ms=1234)
    await store.open()
    try:
        for conn in (store.writer, *store._all_readers):
            async with conn.execute("PRAGMA synchronous") as cur:
                assert (await cur.fetchone())[0] == 2  # FULL
            async with conn.execute("PRAGMA busy_timeout") as cur:
                assert (await cur.fetchone())[0] == 1234
            assert conn.isolation_level is None
    finally:
        await store.close()


def test_check_constraint_rejects_unknown_failure_reason(tmp_path: Path) -> None:
    db = tmp_path / "e.db"
    migrate(db)
    conn = sqlite3.connect(db)
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO events (id, event_type, idempotency_key, request_hash, payload, status, "
            "next_attempt_at, request_id, received_at, updated_at, failure_reason) "
            "VALUES ('a', 't', 'k', 'h', 'null', 'failed', 0, 'r', 0, 0, 'boom')"
        )
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO events (id, event_type, idempotency_key, request_hash, payload, status, "
            "next_attempt_at, request_id, received_at, updated_at) "
            "VALUES ('a', 't', 'k', 'h', 'null', 'weird', 0, 'r', 0, 0)"
        )


def _seed_for_planner(db: Path, rows: int = 2000) -> None:
    migrate(db)
    conn = sqlite3.connect(db)
    statuses = ("queued", "processing", "completed", "failed")
    conn.executemany(
        "INSERT INTO events (id, event_type, idempotency_key, request_hash, payload, status, "
        "next_attempt_at, request_id, received_at, updated_at, completed_at, lease_expires_at) "
        "VALUES (?, ?, ?, 'h', 'null', ?, ?, 'r', 0, 0, ?, ?)",
        [
            (f"id{i:06d}", f"type{i % 7}", f"k{i}", statuses[i % 4], i, i if i % 4 >= 2 else None,
             i if i % 4 == 1 else None)
            for i in range(rows)
        ],
    )
    conn.commit()
    conn.execute("ANALYZE")
    conn.close()


def test_query_plans_use_indexes(tmp_path: Path) -> None:
    db = tmp_path / "e.db"
    _seed_for_planner(db)
    claim = SqliteEventStore.claim_sql(None, None)
    claim_include = SqliteEventStore.claim_sql(2, None)
    claim_exclude = SqliteEventStore.claim_sql(None, 2)
    _assert_indexed(_plan(db, claim, ("r", 1, 1, 1, 10)))
    _assert_indexed(_plan(db, claim_include, ("r", 1, 1, 1, "type1", "type2", 10)))
    _assert_indexed(_plan(db, claim_exclude, ("r", 1, 1, 1, "type1", "type2", 10)))
    reap = (
        "UPDATE events SET status='queued' WHERE status = 'processing' AND lease_expires_at <= ?"
    )
    _assert_indexed(_plan(db, reap, (5,)))
    reap_select = "SELECT id, lease_owner FROM events WHERE status = 'processing' AND lease_expires_at <= ?"
    _assert_indexed(_plan(db, reap_select, (5,)))
    purge_plan = _plan(db, SqliteEventStore.PURGE_SQL, (100, 10))
    _assert_indexed(purge_plan)
    assert "ix_events_purge" in purge_plan
    for event_type in (None, "type1"):
        for before in (None, "id001000"):
            sql, index = SqliteEventStore.list_sql(event_type, before)
            params = ["failed", *([event_type] if event_type else []), *([before] if before else []), 51]
            plan = _plan(db, sql, tuple(params))
            _assert_indexed(plan)
            assert index in plan


async def test_slow_write_is_logged(tmp_path: Path, logs: LogCapture) -> None:
    observed: list[tuple[str, float]] = []
    store = SqliteEventStore(
        str(tmp_path / "e.db"), slow_write_ms=50, on_write=lambda op, s: observed.append((op, s))
    )
    await store.open()
    try:

        async def slow(conn):
            await asyncio.sleep(0.08)
            await conn.execute("SELECT 1")

        await store._write("insert", slow)
    finally:
        await store.close()
    slow_lines = logs.events("db_write_slow")
    assert slow_lines and slow_lines[0]["op"] == "insert"
    assert slow_lines[0]["lock_held_ms"] > 50
    assert observed and observed[0][0] == "insert" and observed[0][1] >= 0.08
