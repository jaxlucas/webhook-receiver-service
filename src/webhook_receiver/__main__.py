"""Entrypoint: ``python -m webhook_receiver``. One Uvicorn worker per container, Uvicorn access log off."""

from __future__ import annotations

import os

import uvicorn

from webhook_receiver.config import Settings
from webhook_receiver.main import create_app


def main() -> None:
    settings = Settings()
    app = create_app(settings)
    uvicorn.run(
        app,
        host=os.environ.get("HOST", "0.0.0.0"),
        port=int(os.environ.get("PORT", "8000")),
        log_config=None,
        access_log=False,
        proxy_headers=True,
        forwarded_allow_ips=os.environ.get("FORWARDED_ALLOW_IPS", "*"),
        # HTTP drain, then the lifespan waits SHUTDOWN_GRACE; keep the sum under stop_grace_period.
        timeout_graceful_shutdown=5,
    )


if __name__ == "__main__":
    main()
