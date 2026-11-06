#!/usr/bin/env bash
set -euo pipefail
ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)
cd "$ROOT"
PROFILE=${1:-all}
EVIDENCE=${2:-results/resource-profile-smoke/$(date -u +%Y%m%dT%H%M%SZ)}
FAULTS=${3:-}
[[ $# -le 3 && ( -z "$FAULTS" || "$FAULTS" == --faults ) ]] || {
  echo 'Usage: resource-profile-smoke.sh [profile|all] [fresh-evidence-directory] [--faults]' >&2; exit 2;
}
[[ "$PROFILE" =~ ^[a-z0-9][a-z0-9-]{0,63}$ ]] || { echo 'Invalid profile.' >&2; exit 2; }
mkdir -p "$EVIDENCE"
[[ ! -e "$EVIDENCE/summary.json" && ! -e "$EVIDENCE/initial-status.json" ]] || {
  echo 'Choose a fresh evidence directory.' >&2; exit 2;
}
active=0
cleanup() {
  local result=$?
  trap - EXIT
  if (( active )); then
    bash infrastructure/scripts/cleanup-lab.sh > "$EVIDENCE/exit-cleanup.json" || result=1
  fi
  exit "$result"
}
trap cleanup EXIT
if [[ -n ${RUNNER_API_URL:-} ]]; then
  curl --fail --silent --show-error --connect-timeout 3 --max-time 10 "${RUNNER_API_URL%/}/api/experiments" \
    | jq -e 'all(.[]; .state == "COMPLETED" or .state == "FAILED" or .state == "CANCELLED")' >/dev/null
fi
bash infrastructure/scripts/apply-profile.sh status normal > "$EVIDENCE/initial-status.json"
jq -e '.success' "$EVIDENCE/initial-status.json" >/dev/null
profiles=("$PROFILE")
if [[ "$PROFILE" == all ]]; then
  profiles=(normal network-constrained disk-constrained high-latency burst-load broker-failure zone-degradation combined-degradation packet-loss)
fi
printf '[]\n' > "$EVIDENCE/cases.json"
for name in "${profiles[@]}"; do
  mkdir -p "$EVIDENCE/$name"
  admitted=false
  if bash infrastructure/scripts/apply-profile.sh validate "$name" > "$EVIDENCE/$name/admission.json"; then admitted=true; fi
  supported=true
  active=1
  for attempt in 1 2; do
    if ! bash infrastructure/scripts/apply-profile.sh apply "$name" > "$EVIDENCE/$name/apply-$attempt.json"; then
      jq -e '.success == false and .unsupported == true' "$EVIDENCE/$name/apply-$attempt.json" >/dev/null
      supported=false
    fi
  done
  for attempt in 1 2; do
    bash infrastructure/scripts/cleanup-lab.sh > "$EVIDENCE/$name/cleanup-$attempt.json"
    jq -e '.success' "$EVIDENCE/$name/cleanup-$attempt.json" >/dev/null
  done
  active=0
  jq --arg profile "$name" --argjson supported "$supported" --argjson admitted "$admitted" \
    '. + [{profile:$profile,supported:$supported,admitted:$admitted,doubleApply:true,doubleCleanup:true}]' \
    "$EVIDENCE/cases.json" > "$EVIDENCE/cases.tmp"
  mv -- "$EVIDENCE/cases.tmp" "$EVIDENCE/cases.json"
done
if [[ "$FAULTS" == --faults ]]; then
  mkdir -p "$EVIDENCE/faults"
  for action in STOP_BROKER KILL_BROKER RESTART_BROKER ISOLATE_BROKER ISOLATE_DOMAIN LIMIT_NETWORK ADD_LATENCY ADD_PACKET_LOSS LIMIT_DISK; do
    target=broker-4
    [[ "$action" != ISOLATE_DOMAIN ]] || target=zone-a
    if ! bash infrastructure/scripts/inject-fault.sh validate "$action" "$target" > "$EVIDENCE/faults/$action-validation.json"; then
      jq -e '.success == false and .unsupported == true' "$EVIDENCE/faults/$action-validation.json" >/dev/null
      continue
    fi
    active=1
    bash infrastructure/scripts/inject-fault.sh apply "$action" "$target" > "$EVIDENCE/faults/$action-applied.json"
    jq -e '.success' "$EVIDENCE/faults/$action-applied.json" >/dev/null
    if [[ "$action" == STOP_BROKER || "$action" == KILL_BROKER ]]; then
      # A scheduled normal reset must not require a namespace of our own stopped broker.
      bash infrastructure/scripts/apply-profile.sh apply normal > "$EVIDENCE/faults/$action-normal-while-stopped.json"
      jq -e '.success and .details.observed["broker-4"].deferred' "$EVIDENCE/faults/$action-normal-while-stopped.json" >/dev/null
    fi
    bash infrastructure/scripts/cleanup-lab.sh > "$EVIDENCE/faults/$action-cleanup-1.json"
    bash infrastructure/scripts/cleanup-lab.sh > "$EVIDENCE/faults/$action-cleanup-2.json"
    active=0
  done
fi
bash infrastructure/scripts/cleanup-lab.sh health > "$EVIDENCE/final-health.json"
jq '{success:true,profiles:.,note:"Unsupported profiles are excluded; supported does not mean calibrated."}' \
  "$EVIDENCE/cases.json" > "$EVIDENCE/summary.json"
cat "$EVIDENCE/summary.json"
