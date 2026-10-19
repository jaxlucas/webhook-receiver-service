#!/usr/bin/env bash
# Phase 4 step 5: run the load scenarios against compose (2 replicas + obs) and read acceptance from Prometheus.
#   DURATION=5m scripts/load.sh            # both scenarios, 5 minutes each (the acceptance run)
#   DURATION=1m SCENARIOS=two scripts/load.sh
set -euo pipefail
cd "$(dirname "$0")/.."

DURATION="${DURATION:-5m}"
SCENARIOS="${SCENARIOS:-two kill}"
BASE="${BASE:-http://localhost:${HTTP_PORT:-8080}}"
PROM="${PROM:-http://localhost:${PROMETHEUS_PORT:-9090}}"
LOCUST_IMAGE="${LOCUST_IMAGE:-locustio/locust:2.32.4}"
OUT="${OUT:-loadtest/results}"
mkdir -p "$OUT"

seconds() { case "$1" in *m) echo $(( ${1%m} * 60 ));; *s) echo "${1%s}";; *) echo "$1";; esac; }
promq() {
  curl -fsS "$PROM/api/v1/query" --data-urlencode "query=$1" \
    | python3 -c 'import json,sys; r=json.load(sys.stdin)["data"]["result"]; print(r[0]["value"][1] if r else "n/a")'
}

run_locust() {
  docker run --rm --network host -v "$PWD/loadtest:/mnt/loadtest" "$LOCUST_IMAGE" \
    -f /mnt/loadtest/locustfile.py --headless -H "$BASE" -u 40 -r 40 -t "$DURATION" \
    --csv "/mnt/loadtest/results/$1" --only-summary || echo "locust reported request failures (see above)"
}

report() {
  local name=$1 window=$2
  echo "== $name (window ${window}s)"
  echo "5xx on POST /events:      $(promq "sum(increase(webhook_http_requests_total{route=\"/events\",method=\"POST\",status=~\"5..\"}[${window}s]))")"
  echo "p95 POST latency (s):     $(promq "histogram_quantile(0.95, sum by (le) (rate(webhook_http_request_duration_seconds_bucket{route=\"/events\",method=\"POST\"}[${window}s])))")"
  echo "max oldest queued age (s): $(promq "max_over_time(max(webhook_oldest_queued_age_seconds)[${window}s:5s])")"
  echo "write-lock p99 (s):       $(promq "histogram_quantile(0.99, sum by (le) (rate(webhook_db_write_lock_seconds_bucket[${window}s])))")"
  echo "ingest accepted:          $(promq "sum(increase(webhook_ingest_total{outcome=\"accepted\"}[${window}s]))")"
  echo "processing completed:     $(promq "sum(increase(webhook_processing_total{outcome=\"completed\"}[${window}s]))")"
  echo "leases reaped:            $(promq "sum(increase(webhook_leases_reaped_total[${window}s]))")"
  echo "firing alerts:            $(curl -fsS "$PROM/api/v1/alerts" | python3 -c 'import json,sys; print(sorted({a["labels"]["alertname"] for a in json.load(sys.stdin)["data"]["alerts"] if a["state"]=="firing"}) or "none")')"
}

docker compose --profile obs up -d --build --wait
window=$(( $(seconds "$DURATION") + 30 ))

for scenario in $SCENARIOS; do
  docker compose up -d --scale api=2 --wait api
  sleep 20  # let Prometheus discover both replicas
  if [ "$scenario" = kill ]; then
    ( sleep $(( $(seconds "$DURATION") / 2 )); docker kill "$(docker compose ps -q api | head -1)" >/dev/null; echo "killed one replica" ) &
  fi
  run_locust "$scenario"
  load_end=$SECONDS
  # Drain: the queue must empty within 30 s after the load stops.
  until [ "$(promq 'max(webhook_queue_events{status="queued"}) + max(webhook_queue_events{status="processing"})')" = 0 ]; do
    if [ $((SECONDS - load_end)) -gt 120 ]; then echo "queue did not drain"; break; fi
    sleep 2
  done
  echo "drained $((SECONDS - load_end)) s after load stopped"
  sleep 15
  report "$scenario" "$window" | tee "$OUT/$scenario-report.txt"
  wait || true
done
