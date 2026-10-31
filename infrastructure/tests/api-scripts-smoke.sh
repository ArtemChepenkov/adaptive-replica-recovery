#!/usr/bin/env bash
set -euo pipefail

if [[ $# -gt 2 ]]; then
  printf 'Usage: bash api-scripts-smoke.sh [fresh-evidence-directory] [runner-results-directory]\n' >&2
  exit 2
fi
ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)
cd "$ROOT"
export RUNNER_API_URL=${RUNNER_API_URL:-http://localhost:8080}
API=${RUNNER_API_URL%/}
EVIDENCE=${1:-results/api-scripts-smoke/$(date -u +%Y%m%dT%H%M%SZ)}
RESULTS=${2:-results/test-lab}
SCENARIO=${RUNNER_SMOKE_SCENARIO:-infrastructure/scenarios/runner-smoke.yaml}
RUN=infrastructure/scripts/run-experiment.sh
STOP=infrastructure/scripts/stop-experiment.sh
RESET=infrastructure/scripts/reset-lab.sh

for command in curl jq diff; do
  command -v "$command" >/dev/null || { printf 'Required command is missing: %s\n' "$command" >&2; exit 2; }
done
[[ -f "$SCENARIO" ]] || { printf 'Scenario not found: %s\n' "$SCENARIO" >&2; exit 2; }
mkdir -p "$EVIDENCE"
[[ ! -e "$EVIDENCE/accepted.json" ]] || { printf 'Choose a new evidence directory.\n' >&2; exit 2; }

get() {
  curl --fail-with-body --silent --show-error --connect-timeout 3 --max-time 10 --url "$API$1"
}
active_id=''
cleanup() {
  if [[ -n "$active_id" ]]; then
    bash "$STOP" "$active_id" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT
wait_terminal() {
  local id=$1 destination=$2
  for attempt in {1..45}; do
    get "/api/experiments/$id" > "$destination"
    if jq -e '.state == "COMPLETED" or .state == "FAILED" or .state == "CANCELLED"' "$destination" >/dev/null; then
      return 0
    fi
    sleep 1
  done
  printf 'Experiment did not finish within smoke timeout.\n' >&2
  return 1
}

# This smoke is intentionally restricted to the explicitly simulated Runner.
# Reading the marker happens before any request that changes application state.
get / > "$EVIDENCE/index.html"
grep -q 'data-simulated="true"' "$EVIDENCE/index.html" || {
  printf 'Refusing smoke: Runner home page does not confirm profile test-lab.\n' >&2
  exit 2
}
get /api/experiments > "$EVIDENCE/before.json"
jq -e 'all(.[]; .state == "COMPLETED" or .state == "FAILED" or .state == "CANCELLED")' \
  "$EVIDENCE/before.json" >/dev/null

if [[ -n ${UI_EXPERIMENT_ID:-} ]]; then
  [[ "$UI_EXPERIMENT_ID" =~ ^[a-zA-Z0-9][a-zA-Z0-9_-]{0,127}$ ]] || {
    printf 'Invalid UI_EXPERIMENT_ID.\n' >&2; exit 2;
  }
  get "/api/experiments/$UI_EXPERIMENT_ID" > "$EVIDENCE/ui-experiment.json"
  get "/api/experiments/$UI_EXPERIMENT_ID/events" > "$EVIDENCE/ui-events.json"
  jq -e '.state == "COMPLETED"' "$EVIDENCE/ui-experiment.json" >/dev/null
  jq -e 'any(.events[]; .message == "SIMULATED_TEST_LAB_NO_PHYSICAL_INFRASTRUCTURE")' \
    "$EVIDENCE/ui-events.json" >/dev/null
fi

# Argument errors must be detected locally, without making an HTTP request.
if bash "$STOP" '../not-an-id?reset=true' > "$EVIDENCE/invalid-id.out" 2> "$EVIDENCE/invalid-id.err"; then
  printf 'Stop accepted an invalid ID.\n' >&2; exit 1
fi
if bash "$RUN" "$EVIDENCE/missing-scenario.yaml" > "$EVIDENCE/missing-file.out" 2> "$EVIDENCE/missing-file.err"; then
  printf 'Run accepted a missing file.\n' >&2; exit 1
fi
if bash "$RESET" unexpected > "$EVIDENCE/reset-argument.out" 2> "$EVIDENCE/reset-argument.err"; then
  printf 'Reset accepted an unexpected argument.\n' >&2; exit 1
fi
printf 'schemaVersion: 2\n' > "$EVIDENCE/invalid.yaml"
if bash "$RUN" "$EVIDENCE/invalid.yaml" > "$EVIDENCE/invalid.json" 2> "$EVIDENCE/invalid.err"; then
  printf 'Run accepted invalid YAML.\n' >&2; exit 1
fi
jq -e '.status == 400 and .field == "schemaVersion"' "$EVIDENCE/invalid.json" >/dev/null

bash "$RUN" "$SCENARIO" > "$EVIDENCE/accepted.json"
active_id=$(jq -er '.id' "$EVIDENCE/accepted.json")
if bash "$RUN" "$SCENARIO" > "$EVIDENCE/conflict.json" 2> "$EVIDENCE/conflict.err"; then
  printf 'Second experiment started while the first was active.\n' >&2; exit 1
fi
jq -e '.status == 409' "$EVIDENCE/conflict.json" >/dev/null
if bash "$RESET" > "$EVIDENCE/reset-conflict.json" 2> "$EVIDENCE/reset-conflict.err"; then
  printf 'Reset succeeded while an experiment was active.\n' >&2; exit 1
fi
jq -e '.status == 409' "$EVIDENCE/reset-conflict.json" >/dev/null
wait_terminal "$active_id" "$EVIDENCE/completed.json"
jq -e '.state == "COMPLETED"' "$EVIDENCE/completed.json" >/dev/null
get "/api/experiments/$active_id/events" > "$EVIDENCE/events.json"
get "/api/experiments/$active_id/metrics" > "$EVIDENCE/metrics.json"
get "/api/experiments/$active_id/summary" > "$EVIDENCE/summary.json"
jq -e 'any(.events[]; .message == "SIMULATED_TEST_LAB_NO_PHYSICAL_INFRASTRUCTURE")' \
  "$EVIDENCE/events.json" >/dev/null
jq -e 'length > 0' "$EVIDENCE/metrics.json" >/dev/null
jq -e '.finalState == "COMPLETED" and .cleanup.success and .filesComplete' "$EVIDENCE/summary.json" >/dev/null
mkdir -p "$EVIDENCE/result"
for file in experiment.json metrics.csv decisions.jsonl events.log summary.json; do
  test -f "$RESULTS/$active_id/$file"
  cp -- "$RESULTS/$active_id/$file" "$EVIDENCE/result/$file"
done
[[ $(find "$RESULTS/$active_id" -maxdepth 1 -type f | wc -l) -eq 5 ]]
jq -S . "$EVIDENCE/summary.json" > "$EVIDENCE/api-summary-normalized.json"
jq -S . "$EVIDENCE/result/summary.json" > "$EVIDENCE/file-summary-normalized.json"
diff -u "$EVIDENCE/file-summary-normalized.json" "$EVIDENCE/api-summary-normalized.json"

if [[ -n ${UI_EXPERIMENT_ID:-} ]]; then
  jq -S '.spec | del(.seed)' "$EVIDENCE/ui-experiment.json" > "$EVIDENCE/ui-spec-normalized.json"
  jq -S '.spec | del(.seed)' "$EVIDENCE/completed.json" > "$EVIDENCE/cli-spec-normalized.json"
  diff -u "$EVIDENCE/ui-spec-normalized.json" "$EVIDENCE/cli-spec-normalized.json" > "$EVIDENCE/ui-cli-spec.diff"
  for side in ui cli; do
    source="$EVIDENCE/completed.json"
    [[ "$side" != ui ]] || source="$EVIDENCE/ui-experiment.json"
    jq -S 'del(.id, .createdAt, .updatedAt, .faultAt, .recoveryStartedAt, .recoveryFinishedAt,
      .measurementEndedAt, .spec.seed)' "$source" > "$EVIDENCE/$side-record-normalized.json"
  done
  diff -u "$EVIDENCE/ui-record-normalized.json" "$EVIDENCE/cli-record-normalized.json" > "$EVIDENCE/ui-cli-record.diff"
  jq -e --slurpfile ui "$EVIDENCE/ui-experiment.json" '.spec.seed != $ui[0].spec.seed' \
    "$EVIDENCE/completed.json" >/dev/null
  printf 'PASS: UI and CLI normalized specs match, with different seeds.\n'
fi

# Reuse the server-normalized JSON; only extend warm-up so stop precedes fault.
jq '.spec | .fault.at = "PT10S"' "$EVIDENCE/completed.json" > "$EVIDENCE/cancel.json"
bash "$RUN" "$EVIDENCE/cancel.json" > "$EVIDENCE/cancel-accepted.json"
active_id=$(jq -er '.id' "$EVIDENCE/cancel-accepted.json")
bash "$STOP" "$active_id" > "$EVIDENCE/stop.json"
wait_terminal "$active_id" "$EVIDENCE/cancelled.json"
jq -e '.state == "CANCELLED" and .faultAt == null and .recoveryStartedAt == null' \
  "$EVIDENCE/cancelled.json" >/dev/null
bash "$STOP" "$active_id" > "$EVIDENCE/stop-again.json"
jq -e '.state == "CANCELLED"' "$EVIDENCE/stop-again.json" >/dev/null
active_id=''
bash "$RESET" > "$EVIDENCE/reset.json"
jq -e '.success and .attempted' "$EVIDENCE/reset.json" >/dev/null
get /api/experiments > "$EVIDENCE/after.json"
printf 'PASS: CLI launch, completed result, stop, conflicts, validation and reset. Evidence: %s\n' "$EVIDENCE"
