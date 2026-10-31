#!/usr/bin/env bash
set -euo pipefail
ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)
cd "$ROOT"
PROFILE=${1:-dev}
case "$PROFILE" in dev|final) ;; *) echo 'Usage: lab-up.sh [dev|final]' >&2; exit 2 ;; esac
export MONITORING_PROFILE="$PROFILE"
compose=(docker compose -f infrastructure/docker-compose.yml)
[[ "$PROFILE" != final ]] || compose+=(--profile final)
"${compose[@]}" config --quiet
# All brokers use the same image; build each distinct target only once.
"${compose[@]}" build broker-1 cruise-control
brokers=(broker-1 broker-2 broker-3 broker-4)
[[ "$PROFILE" != final ]] || brokers+=(broker-5 broker-6)
"${compose[@]}" up -d --no-build --wait --wait-timeout "${LAB_WAIT_TIMEOUT:-300}" "${brokers[@]}"
bash infrastructure/scripts/create-topics.sh
"${compose[@]}" up -d --no-build --wait --wait-timeout "${LAB_WAIT_TIMEOUT:-300}" cruise-control
"${compose[@]}" up -d --wait --wait-timeout "${LAB_WAIT_TIMEOUT:-300}" prometheus
echo 'Containers ready. Cruise Control model still needs metric windows and traffic.'
