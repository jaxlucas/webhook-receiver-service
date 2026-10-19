"""Pure-ASGI body-size limit: checks ``Content-Length`` up front and counts bytes while the body streams."""

from __future__ import annotations

from starlette.types import ASGIApp, Message, Receive, Scope, Send

from webhook_receiver.api.problems import PayloadTooLargeError, problem
from webhook_receiver.observability.metrics import Metrics


class BodyLimitMiddleware:
    def __init__(self, app: ASGIApp, max_bytes: int, metrics: Metrics) -> None:
        self.app = app
        self.max_bytes = max_bytes
        self.metrics = metrics

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        for name, value in scope.get("headers", []):
            if name == b"content-length":
                try:
                    declared = int(value)
                except ValueError:
                    declared = 0
                if declared > self.max_bytes:
                    self.metrics.rejected.labels("too_large").inc()
                    response = problem(
                        413,
                        "payload-too-large",
                        "Payload Too Large",
                        f"request body exceeds {self.max_bytes} bytes",
                        request_id=scope.get("state", {}).get("request_id"),
                    )
                    await response(scope, receive, send)
                    return
                break

        received = 0

        async def limited_receive() -> Message:
            nonlocal received
            message = await receive()
            if message["type"] == "http.request":
                received += len(message.get("body", b""))
                if received > self.max_bytes:
                    raise PayloadTooLargeError(self.max_bytes)
            return message

        await self.app(scope, limited_receive, send)
