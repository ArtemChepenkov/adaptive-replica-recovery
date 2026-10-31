#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 || ! -f "$1" || ! -r "$1" ]]; then
  printf 'Usage: bash run-experiment.sh <readable-scenario.yaml-or.json>\n' >&2
  exit 2
fi

API=${RUNNER_API_URL:-http://localhost:8080}
curl --fail-with-body --silent --show-error --connect-timeout 3 --max-time 30 \
  -H 'Content-Type: application/x-yaml' --data-binary @- \
  --url "${API%/}/api/experiments" < "$1"
printf '\n'
