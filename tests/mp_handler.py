"""Test handler for the multi-process exactly-once test: one ``handler_invocations`` row per invocation.

Loaded in the spawned Uvicorn processes via ``HANDLER_PLUGINS=tests.mp_handler:register``. The side table
lives in its own SQLite file (``INVOCATIONS_DB``): dozens of handler threads writing into the events DB
would starve the replicas' single writers through SQLite's sleep-based busy handler, which measures the
test harness rather than the service.
"""

from __future__ import annotations

import asyncio
import os
import random
import sqlite3
import time
from typing import Any

from webhook_receiver.processing.handler import Event, HandlerContext, HandlerSpec
from webhook_receiver.processing.registry import HandlerRegistry


def _record_invocation(db_path: str, receipt_id: str, replica: str) -> None:
    # Harness-side retry: the side DB is hammered by every handler thread of every process.
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        try:
            conn = sqlite3.connect(db_path, timeout=0, isolation_level=None)
            try:
                conn.execute("PRAGMA synchronous=OFF")
                conn.execute(
                    "INSERT INTO handler_invocations (receipt_id, replica) VALUES (?, ?)",
                    (receipt_id, replica),
                )
                return
            finally:
                conn.close()
        except sqlite3.OperationalError:
            time.sleep(random.uniform(0.001, 0.005))
    raise RuntimeError("could not record the invocation")


async def invocation_recorder(event: Event, ctx: HandlerContext) -> Any:
    await asyncio.sleep(random.uniform(0.05, 0.2))
    await ctx.run_blocking(
        _record_invocation, os.environ["INVOCATIONS_DB"], event.receipt_id, os.environ["REPLICA_ID"]
    )
    await asyncio.sleep(random.uniform(0.0, 0.1))
    return {"handled_by": "mp-test"}


def register(registry: HandlerRegistry) -> None:
    registry.set_default(invocation_recorder, HandlerSpec("mp-test", 30, 128))
