#!/usr/bin/env bash
set -euo pipefail

if [[ $# -gt 2 ]]; then
  printf 'Usage: bash monitoring-smoke.sh [dev|final] [fresh-evidence-directory]\n' >&2
  exit 2
fi
PROFILE=${1:-dev}
case "$PROFILE" in
  dev) BROKERS=4 ;;
  final) BROKERS=6 ;;
  *) printf 'Usage: bash monitoring-smoke.sh [dev|final] [fresh-evidence-directory]\n' >&2; exit 2 ;;
esac
ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)
cd "$ROOT"
API=${PROMETHEUS_URL:-http://localhost:9091}
API=${API%/}
EVIDENCE=${2:-results/monitoring-smoke/$(date -u +%Y%m%dT%H%M%SZ)}
WAIT=${MONITORING_WAIT_SECONDS:-90}
[[ "$WAIT" =~ ^[1-9][0-9]{0,3}$ ]] || { printf 'Invalid MONITORING_WAIT_SECONDS.\n' >&2; exit 2; }
for command in curl jq; do
  command -v "$command" >/dev/null || { printf 'Required command is missing: %s\n' "$command" >&2; exit 2; }
done
mkdir -p "$EVIDENCE"
[[ ! -e "$EVIDENCE/targets.json" ]] || { printf 'Choose a fresh evidence directory.\n' >&2; exit 2; }

query() {
  curl --fail-with-body --silent --show-error --connect-timeout 3 --max-time 10 \
    --get --data-urlencode "query=$1" --url "$API/api/v1/query"
}
valid_values='all(.data.result[]; (.value[1] | tonumber) as $v | ($v | isfinite) and $v >= 0)'

# Read only: this smoke never creates topics, changes a broker, or stops a service.
# Count the expected target identities and values, not unrelated/self-scrape series.
deadline=$((SECONDS + WAIT))
while :; do
  if query 'up{job=~"kafka|cruise-control"}' > "$EVIDENCE/up.json" \
      && jq -e --argjson brokers "$BROKERS" '
        .status == "success" and .data.resultType == "vector"
        and ([.data.result[] | select(.metric.job == "kafka")] | length == $brokers)
        and ([.data.result[] | select(.metric.job == "kafka") | .metric.broker_id | tonumber] | sort == [range(1; $brokers+1)])
        and ([.data.result[] | select(.metric.job == "cruise-control")] | length == 1)
        and all(.data.result[]; .value[1] == "1")' "$EVIDENCE/up.json" >/dev/null \
      && query 'broker_network_out_bytes_total{job="kafka"}' > "$EVIDENCE/ready-payload.json" \
      && jq -e --argjson brokers "$BROKERS" '.status == "success" and (.data.result | length == $brokers)' \
        "$EVIDENCE/ready-payload.json" >/dev/null; then
    break
  fi
  (( SECONDS < deadline )) || { printf 'Expected healthy monitoring targets did not become ready.\n' >&2; exit 1; }
  sleep 2
done
curl --fail-with-body --silent --show-error --connect-timeout 3 --max-time 10 \
  --url "$API/api/v1/targets?state=active" > "$EVIDENCE/targets.json"
curl --fail-with-body --silent --show-error --connect-timeout 3 --max-time 10 \
  --url "$API/api/v1/status/config" > "$EVIDENCE/config.json"
jq -e '.status == "success" and (.data.yaml | test("scrape_interval: 5s"))' "$EVIDENCE/config.json" >/dev/null
curl --fail-with-body --silent --show-error --connect-timeout 3 --max-time 10 \
  --url "$API/api/v1/status/flags" > "$EVIDENCE/flags.json"
jq -e '.status == "success" and (.data["storage.tsdb.retention.time"] | test("^(1d|24h|24h0m0s)$"))' \
  "$EVIDENCE/flags.json" >/dev/null

for metric in broker_network_out_bytes_total broker_network_in_bytes_total broker_under_replicated_partitions; do
  query "$metric{job=\"kafka\"}" > "$EVIDENCE/$metric.json"
  jq -e --argjson brokers "$BROKERS" \
    ".status == \"success\" and (.data.result | length == \$brokers) and $valid_values" \
    "$EVIDENCE/$metric.json" >/dev/null
done
query 'broker_request_latency_ms{job="kafka"}' > "$EVIDENCE/request-latency.json"
jq -e ".status == \"success\" and (.data.result | length > 0) and $valid_values" "$EVIDENCE/request-latency.json" >/dev/null
query 'broker_replica_fetcher_lag{job="kafka"}' > "$EVIDENCE/fetcher-lag.json"
jq -e ".status == \"success\" and (.data.result | length > 0) and $valid_values" "$EVIDENCE/fetcher-lag.json" >/dev/null
query 'cruise_control_executor_replica_actions{job="cruise-control"}' > "$EVIDENCE/replica-actions.json"
jq -e ".status == \"success\" and (.data.result | length == 18) and $valid_values" "$EVIDENCE/replica-actions.json" >/dev/null
query 'cruise_control_executor_partition_data_movement_mebibytes_per_second{job="cruise-control"}' > "$EVIDENCE/movement-rate.json"
jq -e ".status == \"success\" and (.data.result | length == 1) and $valid_values" "$EVIDENCE/movement-rate.json" >/dev/null
query 'sum(cruise_control_executor_replica_actions{job="cruise-control",type=~"replica-action|intra-broker-replica-action",state=~"in-progress|aborting"})' \
  > "$EVIDENCE/active-replica-actions.json"
jq -e ".status == \"success\" and (.data.result | length == 1) and $valid_values" "$EVIDENCE/active-replica-actions.json" >/dev/null
printf 'PASS: %s broker exporters, Cruise Control, metric contracts, 5s scrape and 24h retention. Evidence: %s\n' "$BROKERS" "$EVIDENCE"
