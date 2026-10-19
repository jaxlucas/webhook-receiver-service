"""Phase 1 steps 4–6: validation, size limit, dedup, status lookup, 503."""

from __future__ import annotations

import asyncio
import json
import sqlite3
import uuid

import pytest

from tests.conftest import AppFactory, Harness

VALID = {"idempotency_key": "k1", "event_type": "order.paid", "payload": {"a": 1}}


def _with(**changes: object) -> dict:
    body = dict(VALID)
    for key, value in changes.items():
        if value is ...:
            body.pop(key)
        else:
            body[key] = value
    return body


INVALID_BODIES = [
    pytest.param(_with(idempotency_key=...), id="missing-key"),
    pytest.param(_with(event_type=...), id="missing-type"),
    pytest.param(_with(payload=...), id="missing-payload"),
    pytest.param(_with(extra=1), id="unknown-field"),
    pytest.param(_with(idempotency_key=""), id="empty-key"),
    pytest.param(_with(idempotency_key="x" * 256), id="long-key"),
    pytest.param(_with(idempotency_key="tab\there"), id="non-printable-key"),
    pytest.param(_with(idempotency_key="zażółć"), id="non-ascii-key"),
    pytest.param(_with(idempotency_key=123), id="numeric-key"),
    pytest.param(_with(event_type=""), id="empty-type"),
    pytest.param(_with(event_type="a" * 101), id="long-type"),
    pytest.param(_with(event_type="has space"), id="bad-type-chars"),
    pytest.param([VALID], id="array-body"),
    pytest.param("string", id="string-body"),
    pytest.param(None, id="null-body"),
]


@pytest.mark.parametrize("body", INVALID_BODIES)
async def test_invalid_bodies_are_422_problems(api: Harness, body: object) -> None:
    response = await api.client.post("/events", json=body)
    assert response.status_code == 422, response.text
    assert response.headers["content-type"].startswith("application/problem+json")
    problem = response.json()
    assert problem["type"] == "validation-error"
    assert problem["status"] == 422
    assert problem["errors"] and {"loc", "msg"} <= set(problem["errors"][0])
    assert problem["request_id"] == response.headers["x-request-id"]


@pytest.mark.parametrize(
    "raw",
    [
        b'{"idempotency_key": "k", "event_type": "t", "payload": NaN}',
        b'{"idempotency_key": "k", "event_type": "t", "payload": {"x": Infinity}}',
        b'{"idempotency_key": "k", "event_type": "t", "payload": [-Infinity]}',
    ],
)
async def test_non_finite_numbers_are_422(api: Harness, raw: bytes) -> None:
    response = await api.client.post("/events", content=raw, headers={"content-type": "application/json"})
    assert response.status_code == 422


async def test_invalid_json_and_wrong_content_type_are_422(api: Harness) -> None:
    bad_json = await api.client.post(
        "/events", content=b"{nope", headers={"content-type": "application/json"}
    )
    assert bad_json.status_code == 422
    wrong_type = await api.client.post(
        "/events", content=json.dumps(VALID).encode(), headers={"content-type": "text/plain"}
    )
    assert wrong_type.status_code == 422
    assert wrong_type.json()["errors"][0]["loc"] == ["header", "content-type"]


@pytest.mark.parametrize("payload", [None, [], [1, "two"], "str", 0, 1.5, True, {"nested": {"x": [1]}}])
async def test_any_json_payload_is_accepted(api: Harness, payload: object) -> None:
    response = await api.post_event(key=f"k-{json.dumps(payload)}", payload=payload)
    assert response.status_code == 202, response.text


async def _body_of_size(size: int) -> bytes:
    prefix = b'{"idempotency_key": "k", "event_type": "t", "payload": "'
    suffix = b'"}'
    return prefix + b"x" * (size - len(prefix) - len(suffix)) + suffix


async def test_body_over_limit_with_content_length_is_413(api: Harness) -> None:
    body = await _body_of_size(262144 + 1)
    response = await api.client.post("/events", content=body, headers={"content-type": "application/json"})
    assert response.status_code == 413
    problem = response.json()
    assert problem["type"] == "payload-too-large"
    assert problem["request_id"] == response.headers["x-request-id"]


async def test_body_over_limit_chunked_is_413(api: Harness) -> None:
    body = await _body_of_size(262144 + 1)

    async def chunks():
        for i in range(0, len(body), 16384):
            yield body[i : i + 16384]

    response = await api.client.post(
        "/events", content=chunks(), headers={"content-type": "application/json"}
    )
    assert "content-length" not in response.request.headers
    assert response.status_code == 413
    assert response.json()["type"] == "payload-too-large"


async def test_body_at_limit_is_accepted(api: Harness) -> None:
    body = await _body_of_size(262144)
    response = await api.client.post("/events", content=body, headers={"content-type": "application/json"})
    assert response.status_code == 202


async def test_new_event_is_202_with_location(api: Harness) -> None:
    response = await api.post_event(payload={"a": 1})
    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "queued"
    assert body["duplicate"] is False
    assert uuid.UUID(body["receipt_id"]).version == 7
    assert response.headers["location"] == f"/events/{body['receipt_id']}"
    assert body["received_at"].endswith("Z")


async def test_same_body_is_200_duplicate_with_original_receipt(api: Harness) -> None:
    first = (await api.post_event(payload={"a": 1, "b": 2})).json()
    again = await api.post_event(payload={"a": 1, "b": 2})
    assert again.status_code == 200
    assert again.json() == {**first, "duplicate": True}


async def test_reordered_keys_are_a_duplicate(api: Harness) -> None:
    await api.client.post(
        "/events",
        content=b'{"idempotency_key":"k","event_type":"t","payload":{"a":1,"b":[1,2]}}',
        headers={"content-type": "application/json"},
    )
    response = await api.client.post(
        "/events",
        content=b'{ "payload": { "b": [1, 2], "a": 1 }, "event_type": "t", "idempotency_key": "k" }',
        headers={"content-type": "application/json"},
    )
    assert response.status_code == 200


async def test_different_payload_is_409_without_leaking_original(api: Harness) -> None:
    first = (await api.post_event(payload={"a": 1})).json()
    response = await api.post_event(payload={"a": 2})
    assert response.status_code == 409
    problem = response.json()
    assert problem["type"] == "idempotency-key-reused"
    assert first["receipt_id"] not in response.text
    assert '"a"' not in problem.get("detail", "")
    assert api.logs.events("event_key_conflict")


async def test_number_formatting_matters(api: Harness) -> None:
    await api.post_event(payload=1)
    assert (await api.post_event(payload=1.0)).status_code == 409


async def test_same_key_other_event_type_is_a_new_event(api: Harness) -> None:
    a = await api.post_event(event_type="a.b", payload=1)
    b = await api.post_event(event_type="c.d", payload=1)
    assert a.status_code == b.status_code == 202
    assert a.json()["receipt_id"] != b.json()["receipt_id"]


async def test_concurrent_identical_posts_create_one_row(api: Harness) -> None:
    responses = await asyncio.gather(*(api.post_event(key="same", payload={"x": 1}) for _ in range(50)))
    codes = sorted(r.status_code for r in responses)
    assert codes.count(202) == 1 and codes.count(200) == 49
    assert len({r.json()["receipt_id"] for r in responses}) == 1
    conn = sqlite3.connect(api.settings.DATABASE_PATH)
    assert conn.execute("SELECT count(*) FROM events").fetchone()[0] == 1


async def test_duplicate_log_line_carries_original_request_id(api: Harness) -> None:
    first = await api.post_event(payload=1, headers={"X-Request-ID": "first-req"})
    await api.post_event(payload=1, headers={"X-Request-ID": "second-req"})
    line = api.logs.events("event_duplicate")[-1]
    assert line["request_id"] == "second-req"
    assert line["original_request_id"] == "first-req"
    assert line["receipt_id"] == first.json()["receipt_id"]


async def test_lock_held_beyond_busy_timeout_is_503(harness_factory: AppFactory) -> None:
    async with harness_factory(start_workers=False, BUSY_TIMEOUT_MS=100) as h:
        blocker = sqlite3.connect(h.settings.DATABASE_PATH, isolation_level=None, timeout=0.1)
        blocker.execute("BEGIN IMMEDIATE")
        try:
            response = await h.post_event(payload=1)
        finally:
            blocker.execute("ROLLBACK")
            blocker.close()
        assert response.status_code == 503
        assert response.headers["retry-after"] == "1"
        assert response.json()["type"] == "unavailable"
        assert h.logs.events("db_unavailable")
        conn = sqlite3.connect(h.settings.DATABASE_PATH)
        assert conn.execute("SELECT count(*) FROM events").fetchone()[0] == 0
        # The sender's retry succeeds once the lock is gone.
        assert (await h.post_event(payload=1)).status_code == 202


async def test_get_event_round_trip(api: Harness) -> None:
    receipt = (await api.post_event(key="rt", payload={"a": 1}, headers={"X-Request-ID": "rt-1"})).json()
    response = await api.client.get(f"/events/{receipt['receipt_id']}")
    assert response.status_code == 200
    body = response.json()
    assert body["receipt_id"] == receipt["receipt_id"]
    assert body["event_type"] == "order.paid"
    assert body["idempotency_key"] == "rt"
    assert body["status"] == "queued"
    assert body["attempts"] == 0
    assert body["lease_expiries"] == 0
    assert body["request_id"] == "rt-1"
    assert body["replay_count"] == 0
    assert body["received_at"] == receipt["received_at"]
    assert body["completed_at"] is None
    assert body["result"] is None and body["last_error"] is None and body["failure_reason"] is None
    assert "payload" not in body


async def test_get_event_malformed_id_is_422_unknown_is_404(api: Harness) -> None:
    assert (await api.client.get("/events/not-a-uuid")).status_code == 422
    missing = await api.client.get(f"/events/{uuid.uuid7()}")
    assert missing.status_code == 404
    assert missing.json()["type"] == "not-found"


async def test_result_and_error_presence_rules(harness: Harness) -> None:
    receipt = (await harness.post_event(payload={"a": 1})).json()["receipt_id"]
    body = await harness.wait_for_status(receipt, {"completed"})
    assert body["result"]["handled_by"] == "stub"
    assert body["last_error"] is None and body["failure_reason"] is None
    assert body["completed_at"] is not None


async def test_lone_surrogate_payload_is_422(api: Harness) -> None:
    raw = b'{"idempotency_key": "k", "event_type": "t", "payload": {"x": "\\ud800"}}'
    response = await api.client.post("/events", content=raw, headers={"content-type": "application/json"})
    assert response.status_code == 422
    assert response.json()["errors"][0]["loc"] == ["body", "payload"]


async def test_disk_full_is_503_and_stores_nothing(api: Harness) -> None:
    store = api.store
    await store._on_writer_thread(
        lambda: store.writer.execute(
            "PRAGMA max_page_count = " + str(store.writer.execute("PRAGMA page_count").fetchone()[0])
        ).fetchall()
    )
    big = {"blob": "x" * 200_000}
    response = await api.post_event(key="full", payload=big)
    assert response.status_code == 503
    assert response.headers["retry-after"] == "1"
    assert response.json()["type"] == "unavailable"
    conn = sqlite3.connect(api.settings.DATABASE_PATH)
    assert conn.execute("SELECT count(*) FROM events").fetchone()[0] == 0
    # The writer thread survived: once space is back, writes work again.
    await store._on_writer_thread(
        lambda: store.writer.execute("PRAGMA max_page_count = 1073741823").fetchall()
    )
    assert (await api.post_event(key="after", payload=1)).status_code == 202
