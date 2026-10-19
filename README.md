# webhook-receiver-service

Receives webhook-style events, answers fast with a receipt, and processes each event **exactly once** in the
background, across restarts and with several replicas. Design: [`.ai/specs/2026-10-19-webhook-receiver-service.md`](.ai/specs/2026-10-19-webhook-receiver-service.md).

```bash
docker compose up -d --build --wait          # nginx on :8080 + 2 replicas sharing one SQLite volume
curl -s localhost:8080/events -H 'content-type: application/json' \
  -d '{"idempotency_key":"order-123-paid","event_type":"order.paid","payload":{"any":"json"}}'
# 202 {"receipt_id":"01a1…","status":"queued","duplicate":false,"received_at":"…"}
curl -s localhost:8080/events/01a1…           # status, attempts, result, last_error, …
```

## ⚠️ One host only

All replicas share **one SQLite file on one local Docker volume on one host**. WAL mode needs shared memory and
reliable POSIX locks: replicas on different hosts, or a volume on NFS/EFS/SMB, **will corrupt the database**.
Startup refuses `nfs*`, `cifs`, `smb*` and `fuse.*` mounts (best effort, `FS_GUARD_ENABLED`). Scaling past one
host means a `PostgresEventStore` behind the existing `EventStore` protocol.

There is **no authentication in v1**: expose the service on a trusted network only.

## API

| Call | Answers |
|---|---|
| `POST /events` | `202` new (with `Location`) · `200` duplicate with the original receipt · `409 idempotency-key-reused` · `422` invalid · `413` > 256 KiB · `503` + `Retry-After: 1` when the DB is busy (nothing stored, retry is safe) |
| `GET /events/{receipt_id}` | Status, `attempts`, `lease_expiries`, `result` (when completed), `last_error`, `failure_reason` (when failed) |
| `GET /events?status=failed[&event_type=…&limit=…&cursor=…]` | Newest first, cursor-paged diagnostic listing (no `result`) |
| `POST /events/{receipt_id}/retry` | Replays a `failed` event with fresh budgets: `202`, or `409 event-not-failed` |
| `GET /health` | Per-replica liveness: DB, schema, dispatcher, queue figures, WAL size (`503` when a check fails) |
| `GET /metrics` | Prometheus, per replica; blocked at nginx |

Dedup is per `(event_type, idempotency_key)` for `IDEMPOTENCY_TTL_SECONDS` (24 h) after the event *finished*.
Errors are RFC 9457 `application/problem+json` with a `request_id`; every response carries `X-Request-ID`.

## Handlers

Handlers are `async (event, ctx) -> JSON`, registered per `event_type` (the stub is the default). They must be
**idempotent** (a crash re-runs them), do **async I/O only** on the loop (`ctx.run_blocking` / `ctx.run_cpu`
otherwise), pass `ctx.idempotency_key` downstream, and raise `PermanentError` only when the same input can never
succeed. Register them with `HANDLER_PLUGINS=module:function` (called with the `HandlerRegistry`). Each
`HandlerSpec` has a `timeout` (≤ 15 min) and a `max_concurrency`. Leases are renewed while handlers run.

## Configuration

Every setting is an environment variable; the main ones (defaults): `DATABASE_PATH=/data/events.db`,
`WORKER_CONCURRENCY=128`, `LEASE_SECONDS=30`, `LEASE_RENEW_INTERVAL_SECONDS=10`, `MAX_ATTEMPTS=4`,
`BACKOFF_BASE_SECONDS=5`, `MAX_LEASE_EXPIRIES=10`, `IDEMPOTENCY_TTL_SECONDS=86400`, `MAX_BODY_BYTES=262144`,
`SHUTDOWN_GRACE=10`, `HANDLER_DEFAULT_TIMEOUT_SECONDS=60`, `LOG_FORMAT=json`, `METRICS_ENABLED=true`. The full list
is `src/webhook_receiver/config.py`.

**Shutdown:** on SIGTERM a replica stops claiming, waits `SHUTDOWN_GRACE` for running handlers, then releases the
rest back to `queued` without touching their counters. Compose sets `stop_grace_period: 20s`; if you raise
`SHUTDOWN_GRACE` for long handlers, raise `stop_grace_period` with it.

**Backup / rollback:** `sqlite3 /data/events.db ".backup /backup/events.db"` (never a plain file copy while
replicas run). An older image refuses to start on a newer schema.

## Development

```bash
make venv test lint            # with uv and Python 3.14
make docker-venv docker-test   # without a local 3.14: runs in python:3.14-slim
make promtool                  # check config/rules + rule unit tests
make smoke                     # compose + obs profile end-to-end smoke test
DURATION=5m make load          # Locust acceptance runs, results read from Prometheus
```

Observability (logs, metrics, alerts, runbook): [`docs/observability.md`](docs/observability.md).
Load-test results: [`docs/load-test.md`](docs/load-test.md).
