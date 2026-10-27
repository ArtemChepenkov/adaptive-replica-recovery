#!/usr/bin/env bash
set -euo pipefail
ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)
cd "$ROOT"
PROFILE=${1:-dev}
case "$PROFILE" in dev) COUNT=4 ;; final) COUNT=6 ;; *) echo 'Usage: compose-smoke.sh [dev|final]' >&2; exit 2 ;; esac
compose=(docker compose -f infrastructure/docker-compose.yml)
[[ "$PROFILE" != final ]] || compose+=(--profile final)
"${compose[@]}" config --quiet
for ((i=1; i<=COUNT; i++)); do
  id=$("${compose[@]}" ps -q "broker-$i")
  [[ -n "$id" ]] || { echo "broker-$i is absent" >&2; exit 1; }
  [[ $(docker inspect --format '{{.State.Health.Status}}' "$id") == healthy ]]
done
address=$("${compose[@]}" port cruise-control 9090)
curl --fail --silent --show-error --max-time 15 \
  "http://$address/kafkacruisecontrol/state?json=true" | \
  jq -e 'has("MonitorState") and has("ExecutorState")' >/dev/null
for topic in recovery-data recovery-control; do
  description=$("${compose[@]}" exec -T broker-1 /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server broker-1:9092 --describe --topic "$topic")
  echo "$description"
  partitions=24
  [[ "$topic" != recovery-control ]] || partitions=1
  grep -Eq "PartitionCount: *$partitions([[:space:]]|$)" <<<"$description"
  grep -Eq 'ReplicationFactor: *3([[:space:]]|$)' <<<"$description"
  [[ "$topic" != recovery-data ]] || grep -q 'min.insync.replicas=2' <<<"$description"
  awk -F 'Isr: ' '/Partition: / {split($2,a,"[[:space:]]"); n=split(a[1],b,","); if(n!=3) exit 1}' <<<"$description"
done
echo "PASS: $COUNT healthy brokers, Cruise Control API and replicated topics"
