"""Service settings. Every tunable is an environment variable of the same name."""

from __future__ import annotations

import secrets
import socket
from typing import Literal

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


def default_replica_id() -> str:
    # A random suffix means a restarted container never inherits its old leases.
    return f"{socket.gethostname()}-{secrets.token_hex(3)}"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(case_sensitive=False, extra="ignore")

    DATABASE_PATH: str = "/data/events.db"
    BUSY_TIMEOUT_MS: int = Field(5000, ge=0)
    READ_POOL_SIZE: int = Field(4, ge=1)
    DB_WRITE_SLOW_MS: float = 50.0

    WORKER_CONCURRENCY: int = Field(128, ge=1)
    CLAIM_BATCH: int = Field(32, ge=1)
    POLL_INTERVAL: float = Field(0.25, gt=0)
    LEASE_SECONDS: float = Field(30.0, gt=0)
    REAP_JITTER_SECONDS: float = Field(5.0, ge=0)
    MAX_ATTEMPTS: int = Field(4, ge=1)
    MAX_LEASE_EXPIRIES: int = Field(10, ge=1)
    BACKOFF_BASE_SECONDS: float = Field(5.0, ge=0)

    IDEMPOTENCY_TTL_SECONDS: float = Field(86400.0, ge=0)
    PURGE_INTERVAL_SECONDS: float = Field(60.0, gt=0)
    PURGE_BATCH: int = Field(1000, ge=1)
    PURGE_VACUUM_PAGES: int = Field(2000, ge=0)

    MAX_BODY_BYTES: int = Field(262144, ge=1)
    SHUTDOWN_GRACE: float = Field(10.0, ge=0)

    LOG_LEVEL: str = "INFO"
    LOG_FORMAT: Literal["json", "console"] = "json"
    METRICS_ENABLED: bool = True
    HEALTH_CACHE_SECONDS: float = Field(2.0, ge=0)

    LEASE_RENEW_INTERVAL_SECONDS: float = Field(10.0, gt=0)
    HANDLER_DEFAULT_TIMEOUT_SECONDS: float = Field(60.0, gt=0)
    LOOP_LAG_WARN_MS: float = Field(200.0, ge=0)
    LOOP_LAG_INTERVAL_SECONDS: float = Field(1.0, gt=0)
    BLOCKING_POOL_SIZE: int = Field(32, ge=1)

    STUB_MIN_SECONDS: float = Field(2.0, ge=0)
    STUB_MAX_SECONDS: float = Field(5.0, ge=0)
    STUB_FAILURE_RATE: float = Field(0.0, ge=0, le=1)
    STUB_PERMANENT_FAILURE_RATE: float = Field(0.0, ge=0, le=1)

    # Comma-separated "module:function" hooks, each called with the HandlerRegistry at startup.
    HANDLER_PLUGINS: str = ""

    REPLICA_ID: str = Field(default_factory=default_replica_id)

    # Startup refuses a database on a network filesystem (see Risks in the spec).
    FS_GUARD_ENABLED: bool = True

    @field_validator("LOG_LEVEL")
    @classmethod
    def _upper_level(cls, value: str) -> str:
        return value.upper()

    def public_dump(self) -> dict[str, object]:
        """Settings safe to log at startup. An allow-list, so new secrets never leak by default."""
        allowed = (
            "DATABASE_PATH",
            "WORKER_CONCURRENCY",
            "CLAIM_BATCH",
            "POLL_INTERVAL",
            "LEASE_SECONDS",
            "MAX_ATTEMPTS",
            "MAX_LEASE_EXPIRIES",
            "BACKOFF_BASE_SECONDS",
            "IDEMPOTENCY_TTL_SECONDS",
            "PURGE_INTERVAL_SECONDS",
            "PURGE_BATCH",
            "MAX_BODY_BYTES",
            "SHUTDOWN_GRACE",
            "LOG_LEVEL",
            "LOG_FORMAT",
            "METRICS_ENABLED",
            "LEASE_RENEW_INTERVAL_SECONDS",
            "HANDLER_DEFAULT_TIMEOUT_SECONDS",
        )
        return {name: getattr(self, name) for name in allowed}
