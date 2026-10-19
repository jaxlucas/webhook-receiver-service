"""Phase 4 step 4: exactly-once across 3 Uvicorn processes sharing one DB file, with and without a kill."""

from __future__ import annotations

import asyncio
import json
import os
import signal
import socket
import sqlite3
import subprocess
import sys
import time
from collections import Counter
from pathlib import Path

import httpx
import pytest
from prometheus_client.parser import text_string_to_metric_families

from webhook_receiver.store.migrate import migrate

pytestmark = pytest.mark.slow

REPO = Path(__file__).resolve().parent.parent
TOTAL = 2000
DUPLICATE_SHARE = 0.10


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class Proc:
    def __init__(self, index: int, db: Path, tmp: Path) -> None:
        self.replica = f"proc-{index}"
        self.port = _free_port()
        self.log_path = tmp / f"{self.replica}.log"
        env = {
            **os.environ,
            "PYTHONPATH": str(REPO),
            "PORT": str(self.port),
            "HOST": "127.0.0.1",
            "DATABASE_PATH": str(db),
            "INVOCATIONS_DB": str(db.with_name("invocations.db")),
            "REPLICA_ID": self.replica,
            "HANDLER_PLUGINS": "tests.mp_handler:register",
            "LEASE_SECONDS": "2",
            "LEASE_RENEW_INTERVAL_SECONDS": "0.6",
            "REAP_JITTER_SECONDS": "0.5",
            "POLL_INTERVAL": "0.1",
            "FS_GUARD_ENABLED": "false",
            "LOG_LEVEL": "INFO",
            "SHUTDOWN_GRACE": "5",
        }
        self.log_file = open(self.log_path, "w")
        self.popen = subprocess.Popen(
            [sys.executable, "-m", "webhook_receiver"],
            env=env,
            stdout=self.log_file,
            stderr=subprocess.STDOUT,
        )

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def log_lines(self) -> list[dict]:
        out = []
        for raw in self.log_path.read_text().splitlines():
            try:
                out.append(json.loads(raw))
            except json.JSONDecodeError:
                pytest.fail(f"{self.replica} wrote a non-JSON log line: {raw!r}")
        return out

    def stop(self) -> None:
        if self.popen.poll() is None:
            self.popen.send_signal(signal.SIGTERM)
            try:
                self.popen.wait(timeout=15)
            except subprocess.TimeoutExpired:
                self.popen.kill()
                self.popen.wait()
        self.log_file.close()


async def _wait_healthy(procs: list[Proc]) -> None:
    async with httpx.AsyncClient() as client:
        for proc in procs:
            deadline = time.monotonic() + 20
            while True:
                try:
                    if (await client.get(proc.url + "/health")).status_code == 200:
                        break
                except httpx.HTTPError:
                    pass
                if time.monotonic() > deadline:
                    raise AssertionError(f"{proc.replica} never became healthy:\n{proc.log_path.read_text()}")
                await asyncio.sleep(0.1)


def _bodies() -> list[dict]:
    unique = int(TOTAL * (1 - DUPLICATE_SHARE))
    bodies = [
        {"idempotency_key": f"key-{i}", "event_type": "mp.test", "payload": {"i": i}} for i in range(unique)
    ]
    dups = [bodies[i * 9 % unique] for i in range(TOTAL - unique)]
    merged: list[dict] = []
    for i, body in enumerate(bodies):
        merged.append(body)
        if i % 9 == 8 and dups:
            merged.append(dups.pop())
    return merged + dups


async def _post_all(procs: list[Proc], bodies: list[dict], kill: Proc | None = None) -> Counter:
    codes: Counter = Counter()
    semaphore = asyncio.Semaphore(48)
    killed_after = len(bodies) // 2

    async with httpx.AsyncClient(timeout=15) as client:

        async def post(i: int, body: dict) -> None:
            async with semaphore:
                if kill is not None and i == killed_after:
                    kill.popen.send_signal(signal.SIGKILL)
                live = [p for p in procs if p.popen.poll() is None]
                for attempt in range(6):
                    proc = live[(i + attempt) % len(live)]
                    try:
                        response = await client.post(proc.url + "/events", json=body)
                    except httpx.HTTPError:
                        live = [p for p in procs if p.popen.poll() is None]
                        continue
                    if response.status_code in (200, 202):
                        codes[response.status_code] += 1
                        return
                    if response.status_code != 503:
                        raise AssertionError(f"unexpected {response.status_code}: {response.text}")
                    await asyncio.sleep(0.2)
                raise AssertionError(f"could not deliver event {i}")

        await asyncio.gather(*(post(i, b) for i, b in enumerate(bodies)))
    return codes


def _wait_all_completed(db: Path, unique: int, timeout: float = 90) -> None:
    deadline = time.monotonic() + timeout
    while True:
        conn = sqlite3.connect(db, timeout=10)
        done = conn.execute("SELECT count(*) FROM events WHERE status = 'completed'").fetchone()[0]
        conn.close()
        if done == unique:
            return
        if time.monotonic() > deadline:
            raise AssertionError(f"only {done}/{unique} events completed")
        time.sleep(0.25)


def _completed_counter(procs: list[Proc]) -> float:
    total = 0.0
    for proc in procs:
        text = httpx.get(proc.url + "/metrics", timeout=5).text
        for family in text_string_to_metric_families(text):
            for sample in family.samples:
                if sample.name == "webhook_processing_total" and sample.labels.get("outcome") == "completed":
                    total += sample.value
    return total


@pytest.fixture
def db(tmp_path: Path) -> Path:
    path = tmp_path / "events.db"
    migrate(path)
    side = sqlite3.connect(path.with_name("invocations.db"))
    side.execute("PRAGMA journal_mode=WAL")
    side.execute("PRAGMA synchronous=OFF")
    side.execute("CREATE TABLE handler_invocations (receipt_id TEXT NOT NULL, replica TEXT NOT NULL)")
    side.commit()
    side.close()
    return path


@pytest.mark.parametrize("kill_one", [False, True], ids=["no-kill", "kill-one"])
def test_exactly_once_across_processes(db: Path, tmp_path: Path, kill_one: bool) -> None:
    procs = [Proc(i, db, tmp_path) for i in range(3)]
    try:
        asyncio.run(_wait_healthy(procs))
        bodies = _bodies()
        unique = len({b["idempotency_key"] for b in bodies})
        victim = procs[1] if kill_one else None
        codes = asyncio.run(_post_all(procs, bodies, victim))
        assert codes[202] + codes[200] == len(bodies)
        if kill_one:
            # A POST the victim committed but never answered is retried elsewhere and becomes a 200.
            assert codes[202] <= unique
        else:
            assert codes[202] == unique
        _wait_all_completed(db, unique)
        completed_metric = _completed_counter([p for p in procs if p is not victim])
    finally:
        for proc in procs:
            proc.stop()

    conn = sqlite3.connect(db)
    rows = conn.execute("SELECT id, status, attempts, lease_expiries, result FROM events").fetchall()
    conn.close()
    side = sqlite3.connect(db.with_name("invocations.db"))
    invocations = Counter(r[0] for r in side.execute("SELECT receipt_id FROM handler_invocations"))
    side.close()
    assert len(rows) == unique, "no lost rows and no extra rows"
    assert all(status == "completed" and result is not None for _, status, _, _, result in rows)

    if not kill_one:
        assert all(attempts == 1 for _, _, attempts, _, _ in rows)
        assert all(invocations[event_id] == 1 for event_id, *_ in rows)
        assert sum(invocations.values()) == unique
        assert completed_metric == unique
        return

    assert victim is not None
    reaped_from_victim = {
        line["receipt_id"]
        for proc in procs
        if proc is not victim
        for line in proc.log_lines()
        if line.get("event") == "lease_reaped" and line.get("previous_owner") == victim.replica
    }
    expiries = {event_id: lease_expiries for event_id, _, _, lease_expiries, _ in rows}
    for event_id, *_ in rows:
        assert invocations[event_id] >= 1
        if invocations[event_id] > 1:
            assert expiries[event_id] >= 1, event_id
            assert event_id in reaped_from_victim, event_id
    # Work owned by the survivors was never re-run.
    assert all(e == 0 or event_id in reaped_from_victim for event_id, e in expiries.items())
