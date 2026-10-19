CREATE TABLE events (
    id               TEXT PRIMARY KEY,
    event_type       TEXT NOT NULL,
    idempotency_key  TEXT NOT NULL,
    request_hash     TEXT NOT NULL,
    payload          TEXT NOT NULL,
    status           TEXT NOT NULL CHECK (status IN ('queued', 'processing', 'completed', 'failed')),
    attempts         INTEGER NOT NULL DEFAULT 0,
    lease_expiries   INTEGER NOT NULL DEFAULT 0,
    next_attempt_at  INTEGER NOT NULL,
    lease_owner      TEXT,
    lease_token      TEXT,
    lease_expires_at INTEGER,
    request_id       TEXT NOT NULL,
    result           TEXT,
    last_error       TEXT,
    failure_reason   TEXT CHECK (failure_reason IS NULL OR failure_reason IN
                        ('permanent_error', 'retries_exhausted', 'timeout', 'lease_expiries_exhausted')),
    replay_count     INTEGER NOT NULL DEFAULT 0,
    received_at      INTEGER NOT NULL,
    updated_at       INTEGER NOT NULL,
    completed_at     INTEGER
);

CREATE UNIQUE INDEX ux_events_type_key ON events (event_type, idempotency_key);
CREATE INDEX ix_events_claim ON events (status, next_attempt_at);
CREATE INDEX ix_events_reap ON events (status, lease_expires_at);
CREATE INDEX ix_events_purge ON events (completed_at) WHERE status IN ('completed', 'failed');
CREATE INDEX ix_events_status_id ON events (status, id);
CREATE INDEX ix_events_status_type_id ON events (status, event_type, id);
