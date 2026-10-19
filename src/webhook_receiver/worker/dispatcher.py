"""Per-replica dispatcher: reap expired leases, claim due events, run handlers, commit fenced outcomes.

It also renews the leases this replica holds (the ``LeaseKeeper`` duty) and releases them on shutdown.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import random
import time
from collections import defaultdict
from concurrent.futures import Executor, ProcessPoolExecutor, ThreadPoolExecutor
from dataclasses import dataclass

import structlog

from webhook_receiver.clock import Clock
from webhook_receiver.config import Settings
from webhook_receiver.observability.logging import get_logger, truncate_error
from webhook_receiver.observability.metrics import Metrics
from webhook_receiver.processing.errors import PermanentError
from webhook_receiver.processing.handler import Event, HandlerContext
from webhook_receiver.processing.registry import HandlerRegistry, Registration
from webhook_receiver.store.base import ClaimedEvent, EventStore, OutcomeResult, StoreUnavailableError

log = get_logger()


def backoff_ms(base_seconds: float, attempts_before: int, rng: random.Random | None = None) -> int:
    """``BACKOFF_BASE · 2^attempts ± 20%``: about 5 s, 10 s, 20 s with the defaults."""
    jitter = (rng or random).uniform(0.8, 1.2)
    return int(base_seconds * 1000 * (2**attempts_before) * jitter)


def _format_seconds(value: float) -> str:
    return f"{value:g}"


@dataclass(slots=True)
class InFlight:
    claimed: ClaimedEvent
    registration: Registration
    ctx: HandlerContext
    lease_expires_at: int
    claimed_at: int
    task: asyncio.Task[None] | None = None
    lost: bool = False
    outcome_pending: bool = False


class Dispatcher:
    def __init__(
        self,
        store: EventStore,
        registry: HandlerRegistry,
        settings: Settings,
        metrics: Metrics,
        clock: Clock,
        rng: random.Random | None = None,
    ) -> None:
        self.store = store
        self.registry = registry
        self.settings = settings
        self.metrics = metrics
        self.clock = clock
        self.rng = rng or random.Random()
        self.replica_id = settings.REPLICA_ID
        self.lease_ms = int(settings.LEASE_SECONDS * 1000)
        self._inflight: dict[str, InFlight] = {}
        self._per_handler: defaultdict[str, int] = defaultdict(int)
        self._wake = asyncio.Event()
        self._stopping = False
        self._loop_task: asyncio.Task[None] | None = None
        self._keeper_task: asyncio.Task[None] | None = None
        self.blocking_executor = ThreadPoolExecutor(
            max_workers=settings.BLOCKING_POOL_SIZE, thread_name_prefix="handler-blocking"
        )
        self._cpu_executor: Executor | None = None
        metrics.worker_slots.set(settings.WORKER_CONCURRENCY)
        for registration in registry.registrations():
            metrics.handler_inflight.labels(registration.spec.name).set(0)

    # ---------------------------------------------------------------- lifecycle

    def cpu_executor(self) -> Executor:
        if self._cpu_executor is None:
            self._cpu_executor = ProcessPoolExecutor(max_workers=os.cpu_count() or 1)
        return self._cpu_executor

    def wake(self) -> None:
        self._wake.set()

    def alive(self) -> bool:
        return self._loop_task is not None and not self._loop_task.done()

    @property
    def inflight_count(self) -> int:
        return len(self._inflight)

    def inflight_for(self, handler_name: str) -> int:
        return self._per_handler[handler_name]

    async def start(self) -> None:
        self._stopping = False
        self._loop_task = asyncio.create_task(self._loop(), name="dispatcher")
        self._keeper_task = asyncio.create_task(self._keeper(), name="lease-keeper")

    async def stop(self, grace: float | None = None) -> None:
        """Stop claiming, wait up to ``grace`` for in-flight handlers, then release their leases."""
        grace = self.settings.SHUTDOWN_GRACE if grace is None else grace
        self._stopping = True
        if self._loop_task is not None:
            self._loop_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._loop_task
        tasks = [inf.task for inf in self._inflight.values() if inf.task is not None]
        if tasks:
            await asyncio.wait(tasks, timeout=grace)
        remaining = [inf for inf in self._inflight.values() if inf.task is not None and not inf.task.done()]
        for inf in remaining:
            inf.ctx.cancelled.set()
        if remaining:
            await asyncio.sleep(0)
        for inf in remaining:
            assert inf.task is not None
            inf.task.cancel()
        if remaining:
            await asyncio.gather(*(inf.task for inf in remaining if inf.task), return_exceptions=True)
            tokens = [inf.claimed.lease_token for inf in remaining]
            try:
                released = await self.store.release(self.replica_id, tokens, self.clock.now_ms())
            except StoreUnavailableError as exc:
                log.error("db_unavailable", op=exc.op, error=truncate_error(exc.error))
                released = 0
            self.metrics.leases_released.inc(released)
            log.info("leases_released", count=released)
        if self._keeper_task is not None:
            self._keeper_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._keeper_task
        self.blocking_executor.shutdown(wait=False, cancel_futures=True)
        if self._cpu_executor is not None:
            self._cpu_executor.shutdown(wait=False, cancel_futures=True)

    # ---------------------------------------------------------------- main loop

    async def _loop(self) -> None:
        while not self._stopping:
            try:
                await self.tick()
            except StoreUnavailableError as exc:
                log.warning("dispatcher_db_unavailable", op=exc.op, error=truncate_error(exc.error))
            except Exception as exc:
                log.error("dispatcher_error", error=truncate_error(repr(exc)), exc_info=True)
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._wake.wait(), self.settings.POLL_INTERVAL)
            self._wake.clear()

    async def tick(self) -> None:
        await self.reap()
        await self.claim()

    async def reap(self) -> None:
        reaped = await self.store.reap(
            self.clock.now_ms(),
            self.settings.MAX_LEASE_EXPIRIES,
            int(self.settings.REAP_JITTER_SECONDS * 1000),
        )
        for lease in reaped:
            self.metrics.leases_reaped.labels(lease.outcome).inc()
            log.warning(
                "lease_reaped",
                receipt_id=lease.id,
                request_id=lease.request_id,
                previous_owner=lease.previous_owner,
                lease_expiries=lease.lease_expiries,
                outcome=lease.outcome,
            )
            if lease.outcome == "failed":
                handler = self.registry.resolve(lease.event_type).spec.name
                self.metrics.processing.labels(handler, "failed").inc()
                self.metrics.failed.labels("lease_expiries_exhausted").inc()
                log.error(
                    "event_failed",
                    receipt_id=lease.id,
                    request_id=lease.request_id,
                    attempts=lease.attempts,
                    lease_expiries=lease.lease_expiries,
                    failure_reason="lease_expiries_exhausted",
                    error="lease expired too many times",
                )

    async def claim(self) -> int:
        """Claim per handler, never more rows than the handler (and the replica) has free slots for."""
        if self._stopping:
            return 0
        claimed_total = 0
        routed = self.registry.routed_types()
        for registration in self.registry.registrations():
            global_free = self.settings.WORKER_CONCURRENCY - len(self._inflight)
            if global_free <= 0:
                break
            spec = registration.spec
            free = min(
                spec.max_concurrency - self._per_handler[spec.name],
                global_free,
                self.settings.CLAIM_BATCH,
            )
            if free <= 0:
                continue
            if registration is self.registry.default:
                include, exclude = None, routed
            else:
                include, exclude = self.registry.types_for(spec.name), None
                if not include:
                    continue
            now = self.clock.now_ms()
            claimed = await self.store.claim(self.replica_id, free, now, self.lease_ms, include, exclude)
            for event in claimed:
                self._spawn(event, registration, now)
            claimed_total += len(claimed)
        return claimed_total

    # ---------------------------------------------------------------- running one event

    def _spawn(self, claimed: ClaimedEvent, registration: Registration, claimed_at: int) -> None:
        loop = asyncio.get_running_loop()
        ctx = HandlerContext(
            receipt_id=claimed.id,
            event_type=claimed.event_type,
            attempt=claimed.attempts + 1,
            deadline=loop.time() + registration.spec.timeout,
            blocking_executor=self.blocking_executor,
            cpu_executor_factory=self.cpu_executor,
        )
        inf = InFlight(
            claimed, registration, ctx, lease_expires_at=claimed.lease_expires_at, claimed_at=claimed_at
        )
        self._inflight[claimed.lease_token] = inf
        name = registration.spec.name
        self._per_handler[name] += 1
        self.metrics.inflight.set(len(self._inflight))
        self.metrics.handler_inflight.labels(name).set(self._per_handler[name])
        self.metrics.queue_wait.observe(max(0, claimed_at - claimed.due_at) / 1000)
        inf.task = asyncio.create_task(self._run(inf), name=f"event-{claimed.id}")

    async def _run(self, inf: InFlight) -> None:
        structlog.contextvars.bind_contextvars(request_id=inf.claimed.request_id, receipt_id=inf.claimed.id)
        try:
            await self.run_one(inf)
        except asyncio.CancelledError:
            if inf.lost:
                self.metrics.processing.labels(inf.registration.spec.name, "lease_lost").inc()
            # Shutdown or lease loss: no outcome. The row is released (shutdown) or reaped (lost lease).
        except Exception as exc:  # defensive: a bug here must not kill the task silently
            log.error("worker_error", error=truncate_error(repr(exc)), exc_info=True)
        finally:
            self._inflight.pop(inf.claimed.lease_token, None)
            name = inf.registration.spec.name
            self._per_handler[name] -= 1
            self.metrics.inflight.set(len(self._inflight))
            self.metrics.handler_inflight.labels(name).set(self._per_handler[name])
            if not self._stopping:
                self._wake.set()

    async def run_one(self, inf: InFlight) -> None:
        claimed, spec = inf.claimed, inf.registration.spec
        event = Event(
            receipt_id=claimed.id,
            event_type=claimed.event_type,
            idempotency_key=claimed.idempotency_key,
            payload=json.loads(claimed.payload),
            attempt=claimed.attempts + 1,
        )
        error: str | None = None
        permanent = False
        timed_out = False
        result_json = ""
        started = time.perf_counter()
        timeout_cm = asyncio.timeout_at(inf.ctx.deadline)
        try:
            async with timeout_cm:
                result = await inf.registration.handler(event, inf.ctx)
            result_json = json.dumps(result, allow_nan=False, separators=(",", ":"), ensure_ascii=False)
        except TimeoutError as exc:
            if timeout_cm.expired():
                timed_out = True
                error = f"handler exceeded {_format_seconds(spec.timeout)}s"
            else:
                error = f"{type(exc).__name__}: {exc}"
        except PermanentError as exc:
            permanent = True
            error = str(exc) or type(exc).__name__
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"
        handler_s = time.perf_counter() - started
        self.metrics.handler_duration.labels(spec.name).observe(handler_s)
        if timed_out:
            self.metrics.handler_timeouts.labels(spec.name).inc()
            log.warning(
                "handler_timeout",
                handler=spec.name,
                timeout_s=spec.timeout,
                attempts=claimed.attempts + 1,
            )

        attempts_after = claimed.attempts + 1
        inf.outcome_pending = True
        now = self.clock.now_ms()
        try:
            reason: str | None = None
            if error is None:
                outcome = await self.store.complete(claimed.id, claimed.lease_token, result_json, now)
                kind = "completed"
            elif permanent or attempts_after >= self.settings.MAX_ATTEMPTS:
                reason = "permanent_error" if permanent else "timeout" if timed_out else "retries_exhausted"
                outcome = await self.store.fail(claimed.id, claimed.lease_token, error, reason, now)
                kind = "failed"
            else:
                delay = backoff_ms(self.settings.BACKOFF_BASE_SECONDS, claimed.attempts, self.rng)
                outcome = await self.store.retry(claimed.id, claimed.lease_token, error, now + delay, now)
                kind = "retry"
        except StoreUnavailableError as exc:
            # The row stays processing; it is reaped after the lease expires and re-run.
            log.error("db_unavailable", op=exc.op, error=truncate_error(exc.error))
            return
        self._record_outcome(inf, kind, outcome, error, reason, handler_s, now)

    def _record_outcome(
        self,
        inf: InFlight,
        kind: str,
        outcome: OutcomeResult | None,
        error: str | None,
        reason: str | None,
        handler_s: float,
        now: int,
    ) -> None:
        claimed, name = inf.claimed, inf.registration.spec.name
        if outcome is None:
            self.metrics.processing.labels(name, "lease_lost").inc()
            log.warning("lease_lost", receipt_id=claimed.id)
            return
        self.metrics.processing.labels(name, kind).inc()
        if kind == "completed":
            end_to_end_ms = now - claimed.received_at
            self.metrics.end_to_end.observe(end_to_end_ms / 1000)
            log.info(
                "event_completed",
                attempts=outcome.attempts,
                handler_ms=round(handler_s * 1000, 1),
                queue_wait_ms=max(0, inf.claimed_at - claimed.due_at),
                end_to_end_ms=end_to_end_ms,
            )
        elif kind == "retry":
            log.warning(
                "event_retry_scheduled",
                attempts=outcome.attempts,
                next_attempt_in_ms=outcome.next_attempt_at - now,
                error=truncate_error(error),
            )
        else:
            assert reason is not None
            self.metrics.failed.labels(reason).inc()
            log.error(
                "event_failed",
                attempts=outcome.attempts,
                lease_expiries=outcome.lease_expiries,
                failure_reason=reason,
                error=truncate_error(error),
            )

    # ---------------------------------------------------------------- lease renewal

    async def _keeper(self) -> None:
        while True:
            await asyncio.sleep(self.settings.LEASE_RENEW_INTERVAL_SECONDS)
            try:
                await self.renew_leases()
            except Exception as exc:
                log.error("lease_keeper_error", error=truncate_error(repr(exc)), exc_info=True)

    async def renew_leases(self) -> None:
        held = {token: inf for token, inf in self._inflight.items() if not inf.lost}
        if not held:
            return
        now = self.clock.now_ms()
        try:
            renewed = await self.store.renew(self.replica_id, list(held), now, self.lease_ms)
        except StoreUnavailableError as exc:
            self.metrics.lease_renewals.labels("error").inc(len(held))
            self._self_fence(held, exc.error)
            return
        for token, inf in held.items():
            if token in renewed:
                inf.lease_expires_at = now + self.lease_ms
                self.metrics.lease_renewals.labels("renewed").inc()
            elif not inf.outcome_pending and token in self._inflight:
                self.metrics.lease_renewals.labels("lost").inc()
                log.warning("lease_lost", receipt_id=inf.claimed.id, request_id=inf.claimed.request_id)
                self._cancel(inf)

    def _self_fence(self, held: dict[str, InFlight], renew_error: str) -> None:
        now = self.clock.now_ms()
        for inf in held.values():
            remaining = inf.lease_expires_at - now
            if remaining < self.lease_ms / 3 and not inf.outcome_pending:
                log.warning(
                    "lease_self_fenced",
                    receipt_id=inf.claimed.id,
                    request_id=inf.claimed.request_id,
                    lease_remaining_ms=remaining,
                    renew_error=truncate_error(renew_error),
                )
                self._cancel(inf)

    def _cancel(self, inf: InFlight) -> None:
        inf.lost = True
        inf.ctx.cancelled.set()
        if inf.task is not None:
            inf.task.cancel()
