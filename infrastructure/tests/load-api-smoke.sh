#!/usr/bin/env bash
set -euo pipefail
ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)
cd "$ROOT"
API=${LOAD_API_URL:-http://localhost:8081}
EVIDENCE=${1:-results/load-smoke/$(date -u +%Y%m%dT%H%M%SZ)}
mkdir -p "$EVIDENCE"
[[ ! -e "$EVIDENCE/start.json" ]] || { echo 'Use a new evidence directory.' >&2; exit 2; }
curl --fail --silent --show-error "$API/api/load/status" | jq -e '.cleanupComplete == true' >/dev/null
started=false
cleanup() {
  if [[ "$started" == true ]]; then
    curl --max-time 15 --silent --show-error -X POST "$API/api/load/stop" >/dev/null || true
  fi
}
trap cleanup EXIT
curl --fail --silent --show-error -X POST -H 'Content-Type: application/json' \
  --data-binary @infrastructure/scenarios/load-smoke.json "$API/api/load/start" > "$EVIDENCE/start.json"
started=true
conflict=$(curl --silent --show-error -o "$EVIDENCE/conflict.json" -w '%{http_code}' \
  -X POST -H 'Content-Type: application/json' --data-binary @infrastructure/scenarios/load-smoke.json \
  "$API/api/load/start")
[[ "$conflict" == 409 ]]
sleep 5
curl --fail --silent --show-error "$API/api/load/metrics/current" > "$EVIDENCE/metrics-first.json"
jq -e '.requestCount > 0 and .p99Ms != null and .offeredRps == 100' "$EVIDENCE/metrics-first.json" >/dev/null
sleep 15
curl --fail --silent --show-error "$API/api/load/metrics/current" > "$EVIDENCE/metrics-second.json"
jq -e '.offeredRps == 250 and .controlSettings.slo.produceP99Ms == 100' "$EVIDENCE/metrics-second.json" >/dev/null
[[ $(jq '.requestCount' "$EVIDENCE/metrics-second.json") -gt $(jq '.requestCount' "$EVIDENCE/metrics-first.json") ]]
for attempt in {1..30}; do
  curl --fail --silent --show-error "$API/api/load/status" > "$EVIDENCE/completed.json"
  [[ $(jq '.cleanupComplete' "$EVIDENCE/completed.json") != true ]] || break
  sleep 1
done
jq -e '.state == "COMPLETED" and .cleanupComplete and .requestCount == 5250 and .outstandingRequests == 0' \
  "$EVIDENCE/completed.json" >/dev/null
curl --fail --silent --show-error "$API/api/load/metrics/current" > "$EVIDENCE/metrics-completed.json"
curl --fail --silent --show-error "$API/api/load/metrics" > "$EVIDENCE/history.json"
cp -- "$(jq -r '.rawLogPath' "$EVIDENCE/completed.json")" "$EVIDENCE/requests.csv"
python3 infrastructure/tests/verify-load-csv.py "$EVIDENCE/metrics-completed.json" \
  "$EVIDENCE/requests.csv" > "$EVIDENCE/csv-verification.json"
# A second run verifies manual stop and restart against the same Kafka cluster.
curl --fail --silent --show-error -X POST -H 'Content-Type: application/json' \
  --data-binary @infrastructure/scenarios/load-smoke.json "$API/api/load/start" > "$EVIDENCE/restart.json"
sleep 2
curl --fail --silent --show-error -X POST "$API/api/load/stop" > "$EVIDENCE/stopped.json"
jq -e '.state == "STOPPED" and .cleanupComplete and .requestCount > 0' "$EVIDENCE/stopped.json" >/dev/null
curl --fail --silent --show-error -X POST "$API/api/load/stop" > "$EVIDENCE/stopped-again.json"
diff "$EVIDENCE/stopped.json" "$EVIDENCE/stopped-again.json"
started=false
echo "PASS: Kafka writes, changing rate, HTTP conflict/restart/stop and CSV percentiles. Evidence: $EVIDENCE"
