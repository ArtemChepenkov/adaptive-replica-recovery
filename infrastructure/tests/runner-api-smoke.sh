#!/usr/bin/env bash
set -euo pipefail
ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)
cd "$ROOT"
API=${RUNNER_API_URL:-http://localhost:8080}
EVIDENCE=${1:-results/runner-api-smoke/$(date -u +%Y%m%dT%H%M%SZ)}
RESULTS=${2:-results/test-lab}
mkdir -p "$EVIDENCE"
[[ ! -e "$EVIDENCE/accepted.json" ]] || { echo 'Choose a new evidence directory.' >&2; exit 2; }
active_id=''
cleanup() {
  if [[ -n "$active_id" ]]; then
    curl --max-time 15 --silent -X POST "$API/api/experiments/$active_id/stop" >/dev/null || true
  fi
}
trap cleanup EXIT
wait_terminal() {
  local id=$1 destination=$2
  for attempt in {1..30}; do
    curl --fail --silent --show-error "$API/api/experiments/$id" > "$destination"
    if jq -e '.state == "COMPLETED" or .state == "FAILED" or .state == "CANCELLED"' "$destination" >/dev/null; then
      return 0
    fi
    sleep 1
  done
  echo 'Experiment did not finish within smoke timeout.' >&2
  return 1
}
curl --fail --silent --show-error "$API/api/profiles" > "$EVIDENCE/profiles.json"
jq -e 'any(.[]; .name == "runner-smoke.yaml")' "$EVIDENCE/profiles.json" >/dev/null
code=$(curl --silent --show-error -o "$EVIDENCE/invalid.json" -w '%{http_code}' \
  -H 'Content-Type: application/x-yaml' --data-binary 'schemaVersion: 2' "$API/api/experiments")
[[ "$code" == 400 ]]
jq -e '.status == 400 and .field == "schemaVersion"' "$EVIDENCE/invalid.json" >/dev/null
code=$(curl --silent --show-error -o "$EVIDENCE/not-found.json" -w '%{http_code}' "$API/api/experiments/missing-task6")
[[ "$code" == 404 ]]
code=$(curl --silent --show-error -o "$EVIDENCE/accepted.json" -w '%{http_code}' \
  -H 'Content-Type: application/x-yaml' --data-binary @infrastructure/scenarios/runner-smoke.yaml "$API/api/experiments")
[[ "$code" == 202 ]]
active_id=$(jq -r '.id' "$EVIDENCE/accepted.json")
code=$(curl --silent --show-error -o "$EVIDENCE/conflict.json" -w '%{http_code}' \
  -H 'Content-Type: application/x-yaml' --data-binary @infrastructure/scenarios/runner-smoke.yaml "$API/api/experiments")
[[ "$code" == 409 ]]
code=$(curl --silent --show-error -o "$EVIDENCE/reset-conflict.json" -w '%{http_code}' -X POST "$API/api/lab/reset")
[[ "$code" == 409 ]]
wait_terminal "$active_id" "$EVIDENCE/completed.json"
jq -e '.state == "COMPLETED"' "$EVIDENCE/completed.json" >/dev/null
curl --fail --silent --show-error "$API/api/experiments/$active_id/metrics" > "$EVIDENCE/metrics.json"
curl --fail --silent --show-error "$API/api/experiments/$active_id/events" > "$EVIDENCE/events.json"
jq -e 'length == 3' "$EVIDENCE/metrics.json" >/dev/null
jq -e 'any(.events[]; .message == "SIMULATED_TEST_LAB_NO_PHYSICAL_INFRASTRUCTURE") and (.decisions | length == 3)' \
  "$EVIDENCE/events.json" >/dev/null
mkdir -p "$EVIDENCE/result"
for file in experiment.json metrics.csv decisions.jsonl events.log summary.json; do
  test -f "$RESULTS/$active_id/$file"
  cp -- "$RESULTS/$active_id/$file" "$EVIDENCE/result/$file"
done
[[ $(find "$RESULTS/$active_id" -maxdepth 1 -type f | wc -l) -eq 5 ]]
jq -e '.finalState == "COMPLETED" and .cleanup.success and .filesComplete' "$EVIDENCE/result/summary.json" >/dev/null
# Cancel while warming up; the simulated fault and recovery must not be started.
sed 's/at: 1s/at: 10s/' infrastructure/scenarios/runner-smoke.yaml > "$EVIDENCE/cancel.yaml"
curl --fail --silent --show-error -H 'Content-Type: application/x-yaml' \
  --data-binary "@$EVIDENCE/cancel.yaml" "$API/api/experiments" > "$EVIDENCE/cancel-accepted.json"
active_id=$(jq -r '.id' "$EVIDENCE/cancel-accepted.json")
curl --fail --silent --show-error -X POST "$API/api/experiments/$active_id/stop" > "$EVIDENCE/stop.json"
wait_terminal "$active_id" "$EVIDENCE/cancelled.json"
jq -e '.state == "CANCELLED" and .faultAt == null and .recoveryStartedAt == null' "$EVIDENCE/cancelled.json" >/dev/null
curl --fail --silent --show-error -X POST "$API/api/experiments/$active_id/stop" > "$EVIDENCE/stop-again.json"
jq -e '.state == "CANCELLED"' "$EVIDENCE/stop-again.json" >/dev/null
active_id=''
curl --fail --silent --show-error -X POST "$API/api/lab/reset" > "$EVIDENCE/reset.json"
jq -e '.success and .attempted' "$EVIDENCE/reset.json" >/dev/null
curl --fail --silent --show-error "$API/api/experiments" > "$EVIDENCE/list.json"
echo "PASS: simulated Runner HTTP lifecycle, cancel/conflicts/reset and five result files. Evidence: $EVIDENCE"
