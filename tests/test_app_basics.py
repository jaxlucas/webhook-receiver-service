"""Phase 1 steps 1–2: boot, request id, access log, log hygiene."""

from __future__ import annotations

import uuid

from tests.conftest import Harness


async def test_app_boots_and_health_is_ok(harness: Harness) -> None:
    response = await harness.client.get("/health")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ok"
    assert body["db"] == "ok"
    assert body["dispatcher"] == "ok"
    assert body["replica_id"] == "test-replica"
    assert set(body["queue"]) == {"queued", "processing", "failed", "oldest_queued_age_s"}
    assert isinstance(body["wal_bytes"], int)


async def test_valid_incoming_request_id_is_echoed(api: Harness) -> None:
    response = await api.client.get("/health", headers={"X-Request-ID": "abc.DEF_123-x"})
    assert response.headers["x-request-id"] == "abc.DEF_123-x"


async def test_invalid_or_missing_request_id_is_replaced_with_uuid7(api: Harness) -> None:
    for headers in ({}, {"X-Request-ID": "has space"}, {"X-Request-ID": "x" * 65}):
        response = await api.client.get("/health", headers=headers)
        rid = response.headers["x-request-id"]
        assert rid != headers.get("X-Request-ID")
        assert uuid.UUID(rid).version == 7


async def test_every_log_line_in_a_request_carries_request_id(api: Harness) -> None:
    response = await api.post_event(payload={"a": 1}, headers={"X-Request-ID": "req-123"})
    assert response.status_code == 202
    lines = [line for line in api.logs.lines() if line.get("request_id") == "req-123"]
    names = {line["event"] for line in lines}
    assert {"event_accepted", "http_request"} <= names
    for line in lines:
        assert {"ts", "level", "event", "replica_id"} <= set(line)
        assert line["replica_id"] == "test-replica"


async def test_access_line_uses_route_template(api: Harness) -> None:
    receipt = (await api.post_event(payload=1)).json()["receipt_id"]
    await api.client.get(f"/events/{receipt}")
    access = [line for line in api.logs.events("http_request") if line["method"] == "GET"]
    assert access[-1]["route"] == "/events/{receipt_id}"
    assert receipt not in access[-1]["route"]
    assert access[-1]["status"] == 200
    assert access[-1]["client_ip"] == "10.0.0.1"
    assert "duration_ms" in access[-1]


async def test_health_and_metrics_are_logged_at_debug(api: Harness) -> None:
    await api.client.get("/health")
    await api.client.get("/metrics")
    lines = [line for line in api.logs.events("http_request") if line["route"] in ("/health", "/metrics")]
    assert lines and all(line["level"] == "debug" for line in lines)


async def test_payload_text_never_appears_in_logs(harness: Harness) -> None:
    secret = "SUPER-SECRET-PAYLOAD-VALUE-9f8e7d"
    response = await harness.post_event(payload={"card": secret})
    await harness.wait_for_status(response.json()["receipt_id"], {"completed"})
    await harness.post_event(payload={"card": secret})  # duplicate
    await harness.post_event(payload={"card": secret + "x"})  # conflict
    assert secret not in harness.logs.text


async def test_unknown_route_is_problem_json_404(api: Harness) -> None:
    response = await api.client.get("/nope", headers={"X-Request-ID": "rid-404"})
    assert response.status_code == 404
    assert response.headers["content-type"].startswith("application/problem+json")
    body = response.json()
    assert body["type"] == "not-found"
    assert body["status"] == 404
    assert body["request_id"] == "rid-404"


async def test_wrong_method_is_problem_json_405(api: Harness) -> None:
    response = await api.client.delete("/events")
    assert response.status_code == 405
    assert response.json()["type"] == "method-not-allowed"
    assert response.json()["request_id"] == response.headers["x-request-id"]


async def test_startup_line_logged(api: Harness) -> None:
    startup = api.logs.events("startup")
    assert startup and startup[0]["schema_version"] == 1
    assert "settings" in startup[0]
