#!/usr/bin/env bash
set -euo pipefail
ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)
cd "$ROOT"
topics=(docker compose -f infrastructure/docker-compose.yml exec -T broker-1
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server broker-1:9092)
"${topics[@]}" --create --if-not-exists --topic __CruiseControlMetrics \
  --partitions 1 --replication-factor 3 --config min.insync.replicas=2 \
  --config cleanup.policy=delete --config retention.ms=18000000
"${topics[@]}" --create --if-not-exists --topic recovery-data \
  --partitions 24 --replication-factor 3 --config min.insync.replicas=2
"${topics[@]}" --create --if-not-exists --topic recovery-control \
  --partitions 1 --replication-factor 3
