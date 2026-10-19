"""Numbered SQL migrations, applied under ``BEGIN IMMEDIATE`` so concurrent replicas apply each once."""

from __future__ import annotations

import re
import sqlite3
from importlib import resources
from pathlib import Path

MIGRATION_RE = re.compile(r"^(\d{4})_[a-z0-9_]+\.sql$")


class SchemaTooNewError(RuntimeError):
    """The database was migrated by newer code. Old code never runs on a newer schema."""


def load_migrations() -> list[tuple[int, str]]:
    folder = resources.files("webhook_receiver.store") / "migrations"
    found: list[tuple[int, str]] = []
    for entry in folder.iterdir():
        match = MIGRATION_RE.match(entry.name)
        if match:
            found.append((int(match.group(1)), entry.read_text(encoding="utf-8")))
    found.sort()
    for expected, (number, _) in enumerate(found, start=1):
        if number != expected:
            raise RuntimeError(f"migration numbering gap: expected {expected:04d}, found {number:04d}")
    return found


LATEST_SCHEMA_VERSION = len(load_migrations())


def split_statements(sql: str) -> list[str]:
    statements: list[str] = []
    buffer = ""
    for line in sql.splitlines(keepends=True):
        buffer += line
        if sqlite3.complete_statement(buffer):
            if buffer.strip():
                statements.append(buffer.strip())
            buffer = ""
    if buffer.strip():
        raise ValueError("incomplete SQL statement at end of migration")
    return statements


def migrate(path: str | Path, busy_timeout_ms: int = 5000) -> int:
    """Bring the database at ``path`` to the latest schema. Returns the resulting ``user_version``."""
    migrations = load_migrations()
    latest = len(migrations)
    conn = sqlite3.connect(str(path), isolation_level=None, timeout=busy_timeout_ms / 1000)
    try:
        conn.execute(f"PRAGMA busy_timeout={int(busy_timeout_ms)}")
        # auto_vacuum only takes effect before the first table exists, and must precede WAL mode.
        has_tables = conn.execute("SELECT count(*) FROM sqlite_master").fetchone()[0] > 0
        if not has_tables:
            conn.execute("PRAGMA auto_vacuum=INCREMENTAL")
        conn.execute("PRAGMA journal_mode=WAL")
        while True:
            conn.execute("BEGIN IMMEDIATE")
            try:
                current = conn.execute("PRAGMA user_version").fetchone()[0]
                if current > latest:
                    raise SchemaTooNewError(
                        f"database schema version {current} is newer than this code ({latest})"
                    )
                if current == latest:
                    conn.execute("COMMIT")
                    return current
                _, sql = migrations[current]
                for statement in split_statements(sql):
                    conn.execute(statement)
                conn.execute(f"PRAGMA user_version={current + 1}")
                conn.execute("COMMIT")
            except BaseException:
                conn.execute("ROLLBACK")
                raise
    finally:
        conn.close()
