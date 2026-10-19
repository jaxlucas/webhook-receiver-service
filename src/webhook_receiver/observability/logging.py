"""structlog setup: one JSON object per line on stdout, with ``ts``, ``level``, ``event`` and ``replica_id``.

Payloads, results and raw request bodies are never logged. Callers pass ids and names only.
"""

from __future__ import annotations

import logging
import sys
from typing import IO, Any

import structlog

MAX_ERROR_LOG_CHARS = 500


def truncate_error(text: str | None, limit: int = MAX_ERROR_LOG_CHARS) -> str | None:
    if text is None:
        return None
    return text if len(text) <= limit else text[: limit - 1] + "…"


def configure_logging(
    level: str = "INFO",
    fmt: str = "json",
    replica_id: str | None = None,
    stream: IO[str] | None = None,
) -> None:
    out = stream if stream is not None else sys.stdout

    def add_replica(_: Any, __: str, event_dict: dict[str, Any]) -> dict[str, Any]:
        if replica_id is not None:
            event_dict.setdefault("replica_id", replica_id)
        return event_dict

    shared: list[Any] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True, key="ts"),
        add_replica,
        structlog.processors.format_exc_info,
    ]
    renderer: Any = (
        structlog.processors.JSONRenderer() if fmt == "json" else structlog.dev.ConsoleRenderer(colors=False)
    )
    level_no = logging.getLevelNamesMapping().get(level.upper(), logging.INFO)

    structlog.configure(
        processors=[*shared, renderer],
        wrapper_class=structlog.make_filtering_bound_logger(level_no),
        logger_factory=structlog.PrintLoggerFactory(file=out),
        cache_logger_on_first_use=False,
    )

    # Route stdlib logging (uvicorn, asyncio) through the same renderer so stdout stays pure JSON.
    handler = logging.StreamHandler(out)
    handler.setFormatter(
        structlog.stdlib.ProcessorFormatter(
            foreign_pre_chain=shared,
            processors=[structlog.stdlib.ProcessorFormatter.remove_processors_meta, renderer],
        )
    )
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level_no)
    for name in ("uvicorn", "uvicorn.error"):
        lg = logging.getLogger(name)
        lg.handlers[:] = []
        lg.propagate = True
    # aiosqlite's DEBUG lines include SQL parameters, i.e. payloads. Never let them through.
    logging.getLogger("aiosqlite").setLevel(logging.WARNING)
    access = logging.getLogger("uvicorn.access")
    access.handlers[:] = []
    access.propagate = False
    access.disabled = True


def get_logger() -> structlog.typing.FilteringBoundLogger:
    return structlog.get_logger()
