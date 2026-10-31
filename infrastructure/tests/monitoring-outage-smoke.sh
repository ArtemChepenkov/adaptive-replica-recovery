#!/usr/bin/env bash
set -euo pipefail
ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)
cd "$ROOT"
[[ $# -le 2 ]] || { echo 'Usage: monitoring-outage-smoke.sh [fresh-evidence-directory] [runner-results-root]' >&2; exit 2; }
API=${RUNNER_API_URL:-http://localhost:8080}
API=${API%/}
PROMETHEUS=${PROMETHEUS_URL:-http://localhost:9091}
PROMETHEUS=${PROMETHEUS%/}
EVIDENCE=${1:-results/monitoring-outage/$(date -u +%Y%m%dT%H%M%SZ)}
RESULTS=${2:-results/test-lab}
compose=(docker compose -f infrastructure/docker-compose.yml)
mkdir -p "$EVIDENCE"
[[ ! -e "$EVIDENCE/accepted.json" ]] || { echo 'Choose a fresh evidence directory.' >&2; exit 2; }
get() { curl --fail-with-body --silent --show-error --connect-timeout 3 --max-time 10 --url "$API$1"; }
active_id=''
prometheus_stopped=0
restore_prometheus() {
  "${compose[@]}" start prometheus || return 1
  for attempt in {1..20}; do
    if curl --fail --silent --show-error --connect-timeout 2 --max-time 3 "$PROMETHEUS/-/ready" >/dev/null 2>&1 \
        && [[ -n "$("${compose[@]}" ps -q --status running prometheus)" ]]; then
      return 0
    fi
    sleep 1
  done
  echo 'Prometheus did not become ready after restoration.' >&2
  return 1
}
cleanup() {
  local result=$?
  trap - EXIT
  if [[ -n "$active_id" ]]; then
    bash infrastructure/scripts/stop-experiment.sh "$active_id" >/dev/null 2>&1 || true
    local stopped=0
    for attempt in {1..8}; do
      if curl --fail --silent --connect-timeout 2 --max-time 3 "$API/api/experiments/$active_id" \
          | jq -e '.state == "COMPLETED" or .state == "FAILED" or .state == "CANCELLED"' >/dev/null; then
        stopped=1
        break
      fi
      sleep 1
    done
    (( stopped )) || { echo 'Own experiment cleanup is still unverified.' >&2; result=1; }
  fi
  if (( prometheus_stopped )); then
    restore_prometheus || result=1
  fi
  exit "$result"
}
trap cleanup EXIT
# This checkpoint stops only this Compose project's Prometheus, never a broker.
# The Runner must have simulated control adapters plus explicit live monitoring.
get / > "$EVIDENCE/index.html"
grep -q 'data-simulated="true"' "$EVIDENCE/index.html"
get /api/experiments > "$EVIDENCE/before.json"
jq -e 'all(.[]; .state == "COMPLETED" or .state == "FAILED" or .state == "CANCELLED")' "$EVIDENCE/before.json" >/dev/null
curl --fail --silent --show-error --connect-timeout 3 --max-time 10 "$PROMETHEUS/-/ready" > "$EVIDENCE/prometheus-before.txt"
test -n "$("${compose[@]}" ps -q --status running prometheus)"
cat > "$EVIDENCE/scenario.yaml" <<'YAML'
schemaVersion: 1
name: simulated-control-live-monitoring-outage
timeout: 2m
slo: {produceP99Ms: 100, maxErrorRate: 0.01}
load: {mode: CONSTANT, messageSizeBytes: 1024, schedule: [{duration: 2m, requestsPerSecond: 100}]}
recovery: {throttlePolicy: AIMD, initialThrottleMiBps: 20, minThrottleMiBps: 1, maxThrottleMiBps: 100}
resources: {profile: test-lab-only}
fault: {action: STOP_BROKER, target: broker-2, at: 1s}
YAML
bash infrastructure/scripts/run-experiment.sh "$EVIDENCE/scenario.yaml" > "$EVIDENCE/accepted.json"
active_id=$(jq -er '.id' "$EVIDENCE/accepted.json")
healthy=0
for attempt in {1..35}; do
  get "/api/experiments/$active_id/metrics" > "$EVIDENCE/metrics-before.json"
  if jq -e 'any(.[]; .monitoring.available == true)' "$EVIDENCE/metrics-before.json" >/dev/null; then healthy=1; break; fi
  get "/api/experiments/$active_id" > "$EVIDENCE/status-before.json"
  jq -e '.state != "COMPLETED" and .state != "FAILED" and .state != "CANCELLED"' "$EVIDENCE/status-before.json" >/dev/null
  sleep 1
done
(( healthy )) || { echo 'No healthy live monitoring snapshot. Use test-lab + live-monitoring=true + recovery-polls=30.' >&2; exit 1; }
get "/api/experiments/$active_id/events" > "$EVIDENCE/events-before.json"
jq -e 'any(.events[]; .message == "SIMULATED_TEST_LAB_NO_PHYSICAL_INFRASTRUCTURE")' "$EVIDENCE/events-before.json" >/dev/null
stopped_at=$(date -u +%FT%TZ)
printf '%s\n' "$stopped_at" > "$EVIDENCE/prometheus-stopped-at.txt"
prometheus_stopped=1
"${compose[@]}" stop --timeout 5 prometheus
completed=0
for attempt in {1..100}; do
  get "/api/experiments/$active_id" > "$EVIDENCE/completed.json"
  if jq -e '.state == "COMPLETED" or .state == "FAILED" or .state == "CANCELLED"' "$EVIDENCE/completed.json" >/dev/null; then completed=1; break; fi
  sleep 1
done
(( completed ))
jq -e '.state == "COMPLETED"' "$EVIDENCE/completed.json" >/dev/null
get "/api/experiments/$active_id/metrics" > "$EVIDENCE/metrics.json"
get "/api/experiments/$active_id/events" > "$EVIDENCE/events.json"
get "/api/experiments/$active_id/summary" > "$EVIDENCE/summary.json"
jq -e --arg stopped "$stopped_at" '
  any(.[]; .monitoring.available == true)
  and any(.[];
    .monitoring.available == false and .produceP99Ms == 90 and .safeState != null
    and .brokerNetworkOutBytesPerSecond == null
    and (.monitoring.errors | index("PROMETHEUS_UNAVAILABLE") != null)
    and ((.monitoring.timestamp | sub("\\.[0-9]+Z$"; "Z") | fromdateiso8601) > ($stopped | fromdateiso8601)))' \
  "$EVIDENCE/metrics.json" >/dev/null
jq -e 'any(.events[]; .message == "MONITORING_UNAVAILABLE")' "$EVIDENCE/events.json" >/dev/null
jq -e '.finalState == "COMPLETED" and .cleanup.success and .filesComplete and .diagnosticDataComplete == false and .produceP99Ms == 90' "$EVIDENCE/summary.json" >/dev/null
mkdir -p "$EVIDENCE/result"
for file in experiment.json metrics.csv decisions.jsonl events.log summary.json; do
  cp -- "$RESULTS/$active_id/$file" "$EVIDENCE/result/$file"
done
[[ $(find "$RESULTS/$active_id" -maxdepth 1 -type f | wc -l) -eq 5 ]]
active_id=''
restore_prometheus
prometheus_stopped=0
echo "PASS: Prometheus outage recorded; simulated SLO/recovery continued to COMPLETED with incomplete diagnostics. Evidence: $EVIDENCE"
