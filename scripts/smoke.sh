#!/usr/bin/env bash
# End-to-end smoke test against docker compose (nginx + 2 replicas, obs profile).
# Phase 1 step 8 + Phase 4 steps 1 and 3: healthy containers, JSON logs, both replicas process events,
# nginx and app share the request id, 413 problem+json through nginx, /metrics blocked, Prometheus up == 1.
set -euo pipefail
cd "$(dirname "$0")/.."

BASE="${BASE:-http://localhost:${HTTP_PORT:-8080}}"
PROM="${PROM:-http://localhost:${PROMETHEUS_PORT:-9090}}"
KEEP="${KEEP:-0}"
fail() { echo "SMOKE FAIL: $*" >&2; exit 1; }
cleanup() { [ "$KEEP" = 1 ] || docker compose --profile obs down -v >/dev/null 2>&1 || true; }
trap cleanup EXIT

docker compose --profile obs up -d --build --wait >/dev/null
echo "stack healthy"

run_id="smoke-$(date +%s)"
receipts=()
for i in $(seq 1 40); do
  body=$(printf '{"idempotency_key":"%s-%d","event_type":"smoke.test","payload":{"i":%d}}' "$run_id" "$i" "$i")
  receipt=$(curl -fsS -H 'content-type: application/json' -H "X-Request-ID: $run_id-$i" -d "$body" "$BASE/events" \
    | python3 -c 'import json,sys; print(json.load(sys.stdin)["receipt_id"])')
  receipts+=("$receipt")
done
echo "posted ${#receipts[@]} events"

deadline=$((SECONDS + 60))
for receipt in "${receipts[@]}"; do
  until [ "$(curl -fsS "$BASE/events/$receipt" | python3 -c 'import json,sys; print(json.load(sys.stdin)["status"])')" = completed ]; do
    [ $SECONDS -lt $deadline ] || fail "event $receipt did not complete"
    sleep 0.5
  done
done
echo "all events completed"

# Duplicate through nginx.
dup_status=$(curl -s -o /dev/null -w '%{http_code}' -H 'content-type: application/json' \
  -d "{\"idempotency_key\":\"$run_id-1\",\"event_type\":\"smoke.test\",\"payload\":{\"i\":1}}" "$BASE/events")
[ "$dup_status" = 200 ] || fail "duplicate returned $dup_status"

# 300 KiB body → nginx's problem+json 413 with a request id.
python3 -c 'import json; print(json.dumps({"idempotency_key":"big","event_type":"t","payload":"x"*300000}))' > /tmp/smoke-big.json
big=$(curl -s -D /tmp/smoke-big.h -H 'content-type: application/json' --data-binary @/tmp/smoke-big.json "$BASE/events")
grep -q '^HTTP/1.1 413' /tmp/smoke-big.h || fail "300 KiB body was not 413"
grep -qi '^content-type: application/problem+json' /tmp/smoke-big.h || fail "413 is not problem+json"
echo "$big" | python3 -c 'import json,sys; b=json.load(sys.stdin); assert b["type"]=="payload-too-large" and b["request_id"]' \
  || fail "413 body: $big"

# /metrics is never public.
[ "$(curl -s -o /dev/null -w '%{http_code}' "$BASE/metrics")" = 404 ] || fail "/metrics reachable through nginx"

# Logs: every app line is JSON, both replicas processed events, nginx and app share the request id.
docker compose logs --no-log-prefix api > /tmp/smoke-api.log
python3 - "$run_id" <<'PY' || fail "log checks"
import json, sys
run_id = sys.argv[1]
replicas, completed_by = set(), set()
for raw in open("/tmp/smoke-api.log"):
    raw = raw.strip()
    if not raw:
        continue
    line = json.loads(raw)  # raises on any non-JSON line
    replicas.add(line.get("replica_id"))
    if line.get("event") == "event_completed" and str(line.get("request_id", "")).startswith(run_id):
        completed_by.add(line["replica_id"])
assert len(completed_by) >= 2, f"events processed by {completed_by}"
print("replicas processing:", sorted(completed_by))
PY
docker compose logs --no-log-prefix nginx > /tmp/smoke-nginx.log
grep -q "\"request_id\":\"$run_id-7\"" /tmp/smoke-nginx.log || fail "nginx log lacks the request id"
grep -q "\"request_id\": \"$run_id-7\"" /tmp/smoke-api.log || fail "app log lacks the request id"
echo "nginx and app logs share request ids"

# Prometheus sees both replicas.
deadline=$((SECONDS + 60))
until [ "$(curl -fsS "$PROM/api/v1/query" --data-urlencode 'query=sum(up{job="webhook"})' \
  | python3 -c 'import json,sys; r=json.load(sys.stdin)["data"]["result"]; print(int(float(r[0]["value"][1])) if r else 0)')" = 2 ]; do
  [ $SECONDS -lt $deadline ] || fail "Prometheus does not see up == 1 for both replicas"
  sleep 2
done
echo "prometheus: up == 1 for both replicas"
echo "SMOKE OK"
