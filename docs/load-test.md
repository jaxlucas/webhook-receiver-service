# Load test

`DURATION=5m scripts/load.sh` with `loadtest/locustfile.py`: 2,000 events/min (33 req/s), 10 % duplicates, 1 %
invalid, through nginx to 2 replicas with the `obs` profile; acceptance figures are read from Prometheus.

## Results — 2026-10-19

Hardware: one laptop (ThinkPad P1 Gen 2, local NVMe SSD, Docker 27.3), so Locust, nginx, both replicas and
Prometheus shared the same host. Stub handler: 2–5 s per event. `synchronous=FULL`.

| Acceptance criterion | Two replicas, 5 min | One of two killed halfway, 5 min |
|---|---|---|
| 0 5xx for valid requests | ✅ 0 (10,000 requests, 0 Locust failures) | ✅ 0 (9,971 requests, 0 failures) |
| p95 `POST /events` < 100 ms | ✅ 54 ms (Prometheus), 77 ms client-side | ✅ 49 ms (Prometheus), 74 ms client-side |
| `max(webhook_oldest_queued_age_seconds)` < 10 s | ✅ 0 s | ✅ 0.75 s |
| Queue drained ≤ 30 s after load stops | ✅ 8 s | ✅ 14 s |
| No alert except `WebhookLeaseReaping` during the kill | ✅ none | ✅ none firing at report time |
| Write-lock hold p99 (recorded) | 21 ms | 23 ms |

After both runs the database held 17,768 events, all `completed`, every one with `attempts = 1`. 70 had
`lease_expiries = 1`: the events the killed replica held, reaped and completed by the survivor (exactly the
effectively-once behaviour of Q5). The client saw a ~2.1 s max latency during the kill (nginx failing over).

A first run reported `leases reaped: 0` for the kill scenario although the replica reaped 70: a labelled counter
series created at its first increment is invisible to `increase()`. All bounded label values are now
pre-created at 0, so `WebhookLeaseReaping` and the other counter alerts see the first event.

## Write-throughput ceiling

The multi-process exactly-once test (`tests/test_multiprocess.py`, 3 processes, 2,000 events posted as fast as
possible, ~150 events/s) found the real limit before this test did:

- `synchronous=FULL` makes every commit an fsync **while the cross-process write lock is held**: 6.8 ms on this
  SSD, i.e. **~145 commits/s for all replicas together**, against ~70–100 write transactions/s at the
  acceptance rate (50–70 % lock utilisation) and ~400/s in the burst test.
- SQLite's busy handler backs off to 100 ms sleeps and is unfair between processes: lock-acquisition waits had
  p99 130–330 ms and stalls up to 3.1 s. Queued lease renewals missed their lease and about 25 % of events ran
  twice.

Changes inside `store/` (the spec's contained-fallback seam):

1. Writes run on a dedicated stdlib `sqlite3` writer thread, a whole transaction per call (the spec's
   documented fallback for `aiosqlite` hold times).
2. **Group commit**: writes queued together share one `BEGIN IMMEDIATE … COMMIT`, one SAVEPOINT per operation,
   so one fsync covers the batch (`DB_WRITE_BATCH_MAX`, default 64). Durability is unchanged: a caller is
   answered only after its batch commits.
3. `BEGIN IMMEDIATE` polls for the lock every 0.5–2 ms (bounded by `BUSY_TIMEOUT_MS`) instead of SQLite's
   backoff.

With these, the burst test shows 0 reaped leases and every event run exactly once, and the acceptance runs
above show a 21–23 ms write-lock p99.

## Alert thresholds

The shipped starting thresholds held: none fired under acceptance load. `WebhookBacklogGrowing` (> 30 s for 5 m)
has a wide margin over the observed ≤ 0.75 s. Revisit after production traffic.
