"""RFC 9457 ``application/problem+json`` responses and the exception handlers that produce them."""

from __future__ import annotations

from typing import Any

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from webhook_receiver.observability.logging import get_logger, truncate_error
from webhook_receiver.observability.metrics import Metrics
from webhook_receiver.observability.request_id import current_request_id
from webhook_receiver.store.base import StoreUnavailableError

PROBLEM_MEDIA_TYPE = "application/problem+json"
log = get_logger()

STATUS_TYPES = {
    404: ("not-found", "Not Found"),
    405: ("method-not-allowed", "Method Not Allowed"),
    413: ("payload-too-large", "Payload Too Large"),
    422: ("validation-error", "Validation Error"),
}


class ProblemResponse(JSONResponse):
    media_type = PROBLEM_MEDIA_TYPE


def problem(
    status: int,
    type_: str,
    title: str,
    detail: str | None = None,
    *,
    errors: list[dict[str, Any]] | None = None,
    headers: dict[str, str] | None = None,
    extra: dict[str, Any] | None = None,
    request_id: str | None = None,
) -> ProblemResponse:
    body: dict[str, Any] = {"type": type_, "title": title, "status": status}
    if detail is not None:
        body["detail"] = detail
    if errors is not None:
        body["errors"] = errors
    if extra:
        body.update(extra)
    body["request_id"] = request_id or current_request_id()
    return ProblemResponse(body, status_code=status, headers=headers)


class PayloadTooLargeError(StarletteHTTPException):
    """Raised from the size-limited ``receive``. An ``HTTPException`` so FastAPI never re-wraps it as 400."""

    def __init__(self, limit: int) -> None:
        super().__init__(413, f"request body exceeds {limit} bytes")


def validation_errors(raw: list[Any]) -> list[dict[str, Any]]:
    return [{"loc": list(err.get("loc", ())), "msg": str(err.get("msg", ""))} for err in raw]


def validation_problem(errors: list[dict[str, Any]], metrics: Metrics | None) -> ProblemResponse:
    if metrics is not None:
        metrics.rejected.labels("validation").inc()
    return problem(422, "validation-error", "Validation Error", "The request is invalid.", errors=errors)


def unavailable_problem(exc: StoreUnavailableError, metrics: Metrics | None) -> ProblemResponse:
    if metrics is not None:
        metrics.rejected.labels("unavailable").inc()
    log.error("db_unavailable", op=exc.op, error=truncate_error(exc.error))
    return problem(
        503,
        "unavailable",
        "Service Unavailable",
        "The database is busy or unavailable. Nothing was stored; retry later.",
        headers={"Retry-After": "1"},
    )


def install_problem_handlers(app: FastAPI, metrics: Metrics) -> None:
    @app.exception_handler(RequestValidationError)
    async def _validation(_: Request, exc: RequestValidationError) -> ProblemResponse:
        return validation_problem(validation_errors(list(exc.errors())), metrics)

    @app.exception_handler(StarletteHTTPException)
    async def _http(_: Request, exc: StarletteHTTPException) -> ProblemResponse:
        if exc.status_code == 413:
            metrics.rejected.labels("too_large").inc()
        type_, title = STATUS_TYPES.get(exc.status_code, ("about:blank", "HTTP Error"))
        detail = exc.detail if isinstance(exc.detail, str) else None
        return problem(exc.status_code, type_, title, detail, headers=exc.headers)

    @app.exception_handler(StoreUnavailableError)
    async def _unavailable(_: Request, exc: StoreUnavailableError) -> ProblemResponse:
        return unavailable_problem(exc, metrics)
