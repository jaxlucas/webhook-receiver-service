"""Wall-clock access, injectable so tests can move time forward."""

from __future__ import annotations

import time
from datetime import UTC, datetime
from typing import Protocol


class Clock(Protocol):
    def now_ms(self) -> int: ...


class SystemClock:
    def now_ms(self) -> int:
        return time.time_ns() // 1_000_000


def iso_ms(epoch_ms: int | None) -> str | None:
    """Epoch milliseconds → ISO-8601 UTC (``2026-10-19T10:00:00.000Z``)."""
    if epoch_ms is None:
        return None
    moment = datetime.fromtimestamp(epoch_ms / 1000, UTC)
    return moment.isoformat(timespec="milliseconds").replace("+00:00", "Z")
