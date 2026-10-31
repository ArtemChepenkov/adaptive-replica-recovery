#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 || ! "$1" =~ ^[a-zA-Z0-9][a-zA-Z0-9_-]{0,127}$ ]]; then
  printf 'Usage: bash stop-experiment.sh <experiment-id>\n' >&2
  exit 2
fi

API=${RUNNER_API_URL:-http://localhost:8080}
curl --fail-with-body --silent --show-error --connect-timeout 3 --max-time 30 \
  -X POST --url "${API%/}/api/experiments/$1/stop"
printf '\n'
