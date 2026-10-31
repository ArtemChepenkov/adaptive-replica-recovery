#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 0 ]]; then
  printf 'Usage: bash reset-lab.sh\n' >&2
  exit 2
fi

API=${RUNNER_API_URL:-http://localhost:8080}
curl --fail-with-body --silent --show-error --connect-timeout 3 --max-time 90 \
  -X POST --url "${API%/}/api/lab/reset"
printf '\n'
