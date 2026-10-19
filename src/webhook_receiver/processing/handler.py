"""The handler contract."""

from __future__ import annotations

import asyncio
import functools
from collections.abc import Callable
from concurrent.futures import Executor
from dataclasses import dataclass, field
from typing import Any, Protocol

from pydantic import JsonValue

MAX_HANDLER_TIMEOUT_SECONDS = 15 * 60


@dataclass(frozen=True, slots=True)
class Event:
    """What a handler receives. ``payload`` is the decoded JSON value (may be ``None``)."""

    receipt_id: str
    event_type: str
    idempotency_key: str
    payload: JsonValue
    attempt: int
    """1-based number of this handler run."""


@dataclass(frozen=True, slots=True)
class HandlerSpec:
    name: str
    timeout: float
    max_concurrency: int

    def __post_init__(self) -> None:
        if self.timeout <= 0 or self.timeout > MAX_HANDLER_TIMEOUT_SECONDS:
            raise ValueError(
                f"handler {self.name!r}: timeout must be in (0, {MAX_HANDLER_TIMEOUT_SECONDS}] seconds; "
                "longer work belongs to the external-job pattern"
            )
        if self.max_concurrency < 1:
            raise ValueError(f"handler {self.name!r}: max_concurrency must be >= 1")


@dataclass(slots=True)
class HandlerContext:
    receipt_id: str
    event_type: str
    attempt: int
    deadline: float
    """``loop.time()`` at which the handler's timeout fires."""
    cancelled: asyncio.Event = field(default_factory=asyncio.Event)
    """Set on shutdown or lease loss, before the handler task is cancelled."""
    blocking_executor: Executor | None = None
    cpu_executor_factory: Callable[[], Executor] | None = None

    @property
    def idempotency_key(self) -> str:
        """The key to pass to downstream systems. Equal to the receipt id.

        Lease renewal narrows the duplicate-run window but cannot close it; downstream deduplication
        on this key makes a second run harmless.
        """
        return self.receipt_id

    async def run_blocking[T](self, fn: Callable[..., T], /, *args: Any, **kwargs: Any) -> T:
        """Run blocking I/O on a bounded thread pool instead of the event loop."""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self.blocking_executor, functools.partial(fn, *args, **kwargs))

    async def run_cpu[T](self, fn: Callable[..., T], /, *args: Any, **kwargs: Any) -> T:
        """Run CPU-bound work in a process pool. ``fn`` and its arguments must be picklable."""
        loop = asyncio.get_running_loop()
        executor = self.cpu_executor_factory() if self.cpu_executor_factory else None
        return await loop.run_in_executor(executor, functools.partial(fn, *args, **kwargs))


class Handler(Protocol):
    """``async (event, ctx) -> JsonValue``.

    Contract for every handler:

    - **Idempotent.** Processing is effectively-once: the result is committed exactly once, but a run
      interrupted by a crash or a lost lease runs again. Pass ``ctx.idempotency_key`` to every
      downstream system that has side effects.
    - **Async I/O only on the event loop.** Lease renewal shares the loop; blocking it lets leases
      expire and the event runs twice. Use ``ctx.run_blocking`` for blocking calls and ``ctx.run_cpu``
      for CPU-heavy work.
    - **Raise ``PermanentError`` only when the same input can never succeed.** Any other exception is
      retried with backoff (``MAX_ATTEMPTS`` runs in total).
    - The return value must be JSON-serialisable; it is stored as the event's ``result``.
    - ``ctx.cancelled`` is set before cancellation on shutdown or lease loss, for handlers that want to
      stop cleanly between steps.
    """

    async def __call__(self, event: Event, ctx: HandlerContext) -> JsonValue: ...
