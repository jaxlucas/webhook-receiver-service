"""Request-id middleware. Also writes the one access-log line per request and the HTTP metrics."""

from __future__ import annotations

import re
import time
import uuid
from contextvars import ContextVar

import structlog
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from webhook_receiver.observability.logging import get_logger, truncate_error
from webhook_receiver.observability.metrics import Metrics

REQUEST_ID_RE = re.compile(r"^[A-Za-z0-9._-]{1,64}$")
QUIET_ROUTES = frozenset({"/health", "/metrics"})
UNMATCHED_ROUTE = "unmatched"

request_id_var: ContextVar[str | None] = ContextVar("request_id", default=None)
log = get_logger()


def new_request_id() -> str:
    return str(uuid.uuid7())


def accept_request_id(value: str | None) -> str:
    """Use the client's id when it is well-formed; otherwise silently replace it."""
    if value and REQUEST_ID_RE.fullmatch(value):
        return value
    return new_request_id()


def current_request_id() -> str | None:
    return request_id_var.get()


def route_template(scope: Scope) -> str:
    route = scope.get("route")
    path = getattr(route, "path", None)
    return path if isinstance(path, str) else UNMATCHED_ROUTE


class RequestIdMiddleware:
    def __init__(self, app: ASGIApp, metrics: Metrics) -> None:
        self.app = app
        self.metrics = metrics

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        incoming = None
        for name, value in scope.get("headers", []):
            if name == b"x-request-id":
                incoming = value.decode("latin-1")
                break
        request_id = accept_request_id(incoming)
        scope.setdefault("state", {})["request_id"] = request_id
        status_code = 500
        response_started = False
        started = time.perf_counter()

        async def send_with_id(message: Message) -> None:
            nonlocal status_code, response_started
            if message["type"] == "http.response.start":
                response_started = True
                status_code = message["status"]
                headers = [(k, v) for k, v in message.get("headers", []) if k.lower() != b"x-request-id"]
                headers.append((b"x-request-id", request_id.encode("latin-1")))
                message["headers"] = headers
            await send(message)

        token = request_id_var.set(request_id)
        try:
            with structlog.contextvars.bound_contextvars(request_id=request_id):
                try:
                    await self.app(scope, receive, send_with_id)
                except Exception as exc:
                    log.error("unhandled_error", error=truncate_error(repr(exc)), exc_info=exc)
                    if response_started:
                        raise
                    # Imported lazily: problems.py imports this module.
                    from webhook_receiver.api.problems import problem

                    response = problem(500, "internal-error", "Internal Server Error", request_id=request_id)
                    await response(scope, receive, send_with_id)
                finally:
                    self._access(scope, status_code, time.perf_counter() - started)
        finally:
            request_id_var.reset(token)

    def _access(self, scope: Scope, status_code: int, duration_s: float) -> None:
        route = route_template(scope)
        method = scope.get("method", "")
        self.metrics.http_requests.labels(route, method, str(status_code)).inc()
        self.metrics.http_duration.labels(route, method).observe(duration_s)
        client = scope.get("client")
        fields = {
            "method": method,
            "route": route,
            "status": status_code,
            "duration_ms": round(duration_s * 1000, 2),
            "client_ip": client[0] if client else None,
        }
        if route in QUIET_ROUTES and status_code < 500:
            log.debug("http_request", **fields)
        elif status_code >= 500:
            log.warning("http_request", **fields)
        else:
            log.info("http_request", **fields)
