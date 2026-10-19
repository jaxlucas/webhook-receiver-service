# Observability

Three signals, each answering one question:

- **Logs** — what happened to this request or this event? One JSON object per line on stdout.
- **Metrics** — is the service healthy and keeping up? Prometheus, per replica, at `:8000/metrics`.
- **Listing / status** — which events failed and why? `GET /events?status=failed`, `GET /events/{id}`.

Log `event` names, metric names and labels are contracts for dashboards and alerts: change them with the same
care as the API.

## Following one event

The request id (`X-Request-ID`, kept from the client when it matches `^[A-Za-z0-9._-]{1,64}$`, otherwise
generated) is stored on the event. Every worker line for that event carries the id of the POST that created it.

```bash
docker compose logs --no-log-prefix api nginx | grep '<request id>'
```

A duplicate POST logs its own request id plus `original_request_id`.

## Running Prometheus

```bash
docker compose --profile obs up -d --build --wait   # Prometheus on :9090, alerts from deploy/prometheus/alerts.yml
make promtool                                        # config check, rule check, rule unit tests
```

Prometheus finds replicas through Docker DNS (`dns_sd_configs` on `api`). nginx answers `404` for `/metrics`.

## Key queries

**Shared-DB gauges** (`webhook_queue_events`, `webhook_oldest_queued_age_seconds`, `webhook_wal_bytes`) describe the
one database, so every replica reports the same value. Aggregate them with **`max()`, never `sum()`**.

| Question | Query |
|---|---|
| Are we keeping up? | `max(webhook_oldest_queued_age_seconds)` and `histogram_quantile(0.95, sum by (le) (rate(webhook_queue_wait_seconds_bucket[5m])))` |
| Queue depth | `max by (status) (webhook_queue_events)` |
| POST latency p95 | `histogram_quantile(0.95, sum by (le) (rate(webhook_http_request_duration_seconds_bucket{route="/events",method="POST"}[5m])))` |
| Dedup behaviour | `sum by (outcome) (rate(webhook_ingest_total[5m]))` |
| Bad input vs our failures | `sum by (reason) (rate(webhook_rejected_total[5m]))` |
| Processing results | `sum by (outcome) (rate(webhook_processing_total[5m]))` |
| Why events fail | `sum by (reason) (increase(webhook_failed_total[1h]))` |
| Worker usage | `sum(webhook_inflight_handlers) / sum(webhook_worker_slots)` |
| Single-writer pressure | `histogram_quantile(0.99, sum by (le, op) (rate(webhook_db_write_lock_seconds_bucket[5m])))` |
| Crash recovery | `sum by (outcome) (rate(webhook_leases_reaped_total[10m]))` |
| Lease renewal health | `sum by (result) (rate(webhook_lease_renewals_total[5m]))` |
| Loop blocking | `histogram_quantile(0.99, sum by (le) (rate(webhook_event_loop_lag_seconds_bucket[5m])))` |
| Deployed versions | `count by (version, schema_version) (webhook_build_info)` |

`event_type` is never a label (it is client-controlled); per-handler metrics use the registry's handler name.

## Alerts and runbook

| Alert | First steps |
|---|---|
| `WebhookBacklogGrowing` | Check `webhook_inflight_handlers` vs `webhook_worker_slots`. Saturated: add replicas or raise `WORKER_CONCURRENCY`. Not saturated: check the write-lock histogram and `db_write_slow` logs (single-writer ceiling). |
| `WebhookEventsFailing` | `GET /events?status=failed` → look at `failure_reason` and `last_error`. Fix the cause, then `POST /events/{id}/retry`. |
| `WebhookUnavailable` | 503s mean DB busy or disk trouble. Look for `db_unavailable` logs, disk space, and the write-lock p99. Nothing was stored for those requests; senders retry. |
| `WebhookLeaseReaping` | `lease_reaped` logs show `previous_owner`. Crashing replicas: check restarts. Otherwise handlers outlive their lease: check `webhook_lease_renewals_total{result="error"}`. |
| `WebhookReplicaDown` | `docker compose ps`; container logs; the `HEALTHCHECK` result. |
| `WebhookWalLarge` | A reader never finishes, which starves checkpoints. Check for long-running queries or stuck backups. |
| `WebhookLeaseRenewalFailing` | Renewals fail (`lease_self_fenced`, `lease_lost`). Long handlers may run twice; downstream dedup on `ctx.idempotency_key` keeps that harmless. Check DB contention. |
| `WebhookEventLoopBlocked` | A handler does CPU or blocking work on the loop. Look for `event_loop_lag` logs and move that work to `ctx.run_cpu` / `ctx.run_blocking`. |

Disk-space alerting belongs to host monitoring.

## Shutdown and long handlers

`SHUTDOWN_GRACE` (default 10 s) is how long a replica waits for running handlers on SIGTERM. Afterwards `ctx.cancelled`
is set, handlers are cancelled and their leases released to `queued` without changing counters: a redeploy re-runs
long handlers from the start; it never fails them. Containers running long handlers may raise `SHUTDOWN_GRACE`;
keep compose's `stop_grace_period` (20 s) above it, or Docker SIGKILLs the replica first.

## Log events

`http_request`, `event_accepted`, `event_duplicate`, `event_key_conflict`, `event_key_reused_after_ttl`,
`event_completed`, `event_retry_scheduled`, `event_failed`, `event_replayed`, `event_replay_rejected`,
`lease_reaped`, `lease_lost`, `lease_self_fenced`, `handler_timeout`, `event_loop_lag`, `leases_released`,
`db_write_slow`, `db_unavailable`, `purge_run`, `startup`, `shutdown`. Field lists: the spec's Observability section.
Never logged: payloads, results, raw bodies. `last_error` is truncated to 500 characters in logs.
