#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)
requested=${1:-results/acceptance/$(date -u +%Y%m%dT%H%M%SZ)-$$}
[[ "$requested" == /* ]] || requested="$ROOT/$requested"
evidence=$(realpath -m -- "$requested")
[[ "$evidence" == "$ROOT"/results/* && ! -e "$evidence" ]] || {
  echo 'Choose a fresh acceptance directory under results/.' >&2
  exit 2
}
mkdir -p -- "$evidence"
cd /tmp
check="$ROOT/infrastructure/tests/acceptance-check.py"
prepare="$ROOT/infrastructure/tests/prepare-acceptance-data.sh"
source "$ROOT/infrastructure/tests/acceptance-runtime.sh"
active_id=''
compose=()
node_bin=${TASK14_NODE:-node}
ui_script="$ROOT/infrastructure/tests/acceptance-ui.cjs"
playwright_module=${TASK14_PLAYWRIGHT_MODULE:-}

get() { curl -fsS --connect-timeout 3 --max-time 12 "$1"; }
kafka() {
  "${compose[@]}" exec -T -e KAFKA_HEAP_OPTS=-Xmx128m broker-1 "/opt/kafka/bin/kafka-$1.sh" \
    --bootstrap-server broker-1:9092 "${@:2}"
}

terminal() { jq -e '.state == "COMPLETED" or .state == "FAILED" or .state == "CANCELLED"' "$1" >/dev/null; }

cleanup() {
  local result=$? attempt
  set +e
  if [[ -n ${ACCEPTANCE_RUNTIME_DIR:-} ]]; then acceptance_slo_available; fi
  if [[ -n "$active_id" && -n ${RUNNER_API_URL:-} ]]; then
    curl -fsS --max-time 15 -X POST "$RUNNER_API_URL/api/experiments/$active_id/stop" > "$evidence/emergency-stop.json"
    for attempt in {1..60}; do
      get "$RUNNER_API_URL/api/experiments/$active_id" > "$evidence/emergency-state.json" &&
        terminal "$evidence/emergency-state.json" && break
      sleep 2
    done
  fi
  stop_acceptance_runtime || result=1
  exit "$result"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

wait_model() {
  local proof=$1 goal=$2 attempt partitions
  partitions=$(jq '[.topics[].partitions | length] | add' "$proof/fixture.json")
  for attempt in {1..90}; do
    if get "$cc/state?json=true" > "$proof/model-ready.json" &&
      jq -e --arg goal "$goal" --argjson n "$partitions" \
        '.ExecutorState.state == "NO_TASK_IN_PROGRESS" and (.AnalyzerState.readyGoals | index($goal) != null)
        and .MonitorState.numTotalPartitions >= ($n + 15) and .MonitorState.numValidPartitions >= $n' \
        "$proof/model-ready.json" >/dev/null; then return 0; fi
    sleep 3
  done
  echo "Cruise Control did not build a model for $goal" >&2
  return 1
}

watch_experiment() {
  local label=$1 proof=$2 result=$3 fallback=$4 attempt changed=false restored=false
  local api="$RUNNER_API_URL/api/experiments/$active_id"
  for attempt in {1..480}; do
    get "$api" > "$proof/state.json"
    if terminal "$proof/state.json"; then break; fi
    if [[ ( "$label" == stop || "$label" == failure ) && "$changed" == false ]]; then
      if get "$cc/state?json=true" > "$proof/executor-during.json" &&
        jq -e '.ExecutorState.state == "INTER_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS" and
          (.ExecutorState.triggeredUserTaskId != null)' "$proof/executor-during.json" >/dev/null; then
        if [[ "$label" == stop ]]; then
          bash "$ROOT/infrastructure/scripts/stop-experiment.sh" "$active_id" > "$proof/stop.json"
        else
          curl -fsS --max-time 20 -X POST "$load/stop" > "$proof/load-interrupted.json"
        fi
        changed=true
      fi
    elif [[ "$label" == metrics-stale ]]; then
      if [[ "$changed" == false ]]; then
        if get "$cc/recovery_control?json=true" > "$proof/control-before-outage.json" &&
          jq -e '.enabled and .constraintState == "SATISFIED"' "$proof/control-before-outage.json" >/dev/null; then
          acceptance_slo_unavailable
          date -u +%FT%TZ > "$proof/outage-start.txt"
          changed=true
        fi
      elif [[ "$restored" == false && -s "$result/decisions.jsonl" ]] &&
        jq -se 'any(.[]; .constraintState == "METRICS_STALE" and .reason == "METRICS_STALE")' \
          "$result/decisions.jsonl" >/dev/null 2>&1 &&
        get "$cc/recovery_control?json=true" > "$proof/control-during-outage.json" &&
        jq -e --argjson fallback "$fallback" \
          '.enabled and .constraintState == "METRICS_STALE" and .currentThrottleBytesPerSecond == $fallback' \
          "$proof/control-during-outage.json" >/dev/null; then
        acceptance_slo_available
        date -u +%FT%TZ > "$proof/outage-end.txt"
        restored=true
      fi
    fi
    sleep 2
  done
  terminal "$proof/state.json"
  [[ "$label" != stop || "$changed" == true ]]
  [[ "$label" != failure || "$changed" == true ]]
  [[ "$label" != metrics-stale || "$restored" == true ]]
  acceptance_wait_http "$api/summary" "$proof/summary-api.json"
}

verify_cleanup() {
  local proof=$1 removed
  bash "$ROOT/infrastructure/scripts/cleanup-lab.sh" health > "$proof/health-after.json"
  kafka reassign-partitions --list > "$proof/reassignments-after.txt"
  grep -q 'No partition reassignments found' "$proof/reassignments-after.txt"
  kafka configs --describe --entity-type brokers > "$proof/broker-configs-after.txt"
  kafka configs --describe --entity-type topics > "$proof/topic-configs-after.txt"
  if grep -Eq '(leader|follower)\.replication\.throttled\.(rate|replicas)=' \
      "$proof/broker-configs-after.txt" "$proof/topic-configs-after.txt"; then
    echo 'Temporary replication throttles remain after cleanup.' >&2
    return 1
  fi
  get "$cc/state?json=true" > "$proof/executor-after.json"
  removed=$(jq -r '(.ExecutorState.recentlyRemovedBrokers // []) | join(",")' "$proof/executor-after.json")
  if [[ -n "$removed" ]]; then
    curl -fsS --max-time 15 -X POST "$cc/admin?json=true&drop_recently_removed_brokers=$removed" > "$proof/removed-brokers-reset.json"
    get "$cc/state?json=true" > "$proof/executor-after-reset.json"
    jq -e '(.ExecutorState.recentlyRemovedBrokers // []) | length == 0' "$proof/executor-after-reset.json" >/dev/null
  fi
}

run_case() {
  local label=$1 slug goal=ReplicaDistributionGoal seed='' variant='' outcome=COMPLETED fallback=0
  local proof="$evidence/$label" input result task_id
  case "$label" in
    fixed-cli|fixed-ui|stop|failure) slug=smoke-fixed ;;
    method-fixed-conservative|method-fixed-tuned|method-stock-concurrency-adjuster|method-adaptive-throttle-only|method-risk-ordering-only|method-full-method)
      slug=smoke-fixed ;;
    adaptive-[1-5]) slug=smoke-adaptive-step; seed=${label#adaptive-} ;;
    domain-positive|domain-negative) slug=$label; goal=RackAwareDistributionGoal ;;
    metrics-stale|unsatisfiable) slug=$label ;;
    *) echo "Unknown acceptance case: $label" >&2; return 2 ;;
  esac
  case "$label" in
    method-*) variant=${label#method-} ;;
    failure) variant=fixed-conservative; outcome=FAILED ;;
    stop) variant=fixed-conservative; outcome=CANCELLED ;;
  esac
  echo "Running $label"
  mkdir -- "$proof"
  input="$ROOT/infrastructure/scenarios/$slug.yaml"
  if [[ -n "$seed$variant" || "$label" == metrics-stale ]]; then
    python3 "$check" render "$input" "$seed" "$variant" > "$proof/input.json"
    input="$proof/input.json"
  fi
  if [[ "$label" == metrics-stale ]]; then
    fallback=$(jq -r '.recovery.safeFallbackThrottleMiBps * 1048576' "$input")
  fi
  cp -- "$input" "$proof/scenario.yaml"
  bash "$prepare" "$slug" "$proof"
  wait_model "$proof" "$goal"
  bash "$prepare" --verify "$slug" "$proof"
  if [[ "$label" == fixed-ui ]]; then
    (cd "$ROOT" && "$node_bin" "$ui_script" "$RUNNER_API_URL" "$slug.yaml" "$playwright_module" "${TASK14_BROWSER_CHANNEL:-}") \
      > "$proof/accepted.json" 2> "$proof/browser.log" || {
        cat "$proof/browser.log" >&2
        echo 'Browser launch did not return an accepted experiment.' >&2
        return 1
      }
  else
    bash "$ROOT/infrastructure/scripts/run-experiment.sh" "$input" > "$proof/accepted.json"
  fi
  active_id=$(jq -er '.id | select(test("^[A-Za-z0-9_-]+$"))' "$proof/accepted.json")
  result="$RUNNER_RESULTS_DIRECTORY/$active_id"
  watch_experiment "$label" "$proof" "$result" "$fallback"
  active_id=''
  python3 "$check" result "$result" "$label" "$outcome" > "$proof/result.json"
  task_id=$(jq -r 'select(.message | startswith("RECOVERY_TASK ")) | .message | split(" ")[1]' "$result/events.log")
  [[ "$task_id" =~ ^[0-9a-f-]{36}$ ]]
  get "$cc/user_tasks?json=true&user_task_ids=$task_id" > "$proof/cruise-task.json"
  verify_cleanup "$proof"
  if [[ "$label" == domain-* ]]; then
    "${compose[@]}" logs --no-color cruise-control > "$proof/executor.log"
    bash "$prepare" --snapshot "$slug" "$proof"
    python3 "$check" order "$proof/executor.log" "$result" "$proof/fixture.json" "$proof/placement-after.json" > "$proof/ordering.json"
  fi
  cat "$proof/result.json" >> "$evidence/run-index.jsonl"
  echo "PASS $label ($outcome)"
}

order_seed=${TASK14_ORDER_SEED:-20260910}
python3 - "$order_seed" > "$evidence/seed-order.json" <<'PYTHON'
import json
import random
import sys

print(json.dumps(random.Random(int(sys.argv[1])).sample(range(1, 6), 5)))
PYTHON
printf '%s\n' "$order_seed" > "$evidence/order-seed.txt"
if [[ -n ${TASK14_CASES:-} ]]; then
  IFS=, read -ra cases <<< "$TASK14_CASES"
else
  cases=(fixed-cli fixed-ui domain-positive domain-negative metrics-stale unsatisfiable stop failure)
  mapfile -t seeds < <(jq -r '.[]' "$evidence/seed-order.json")
  for seed in "${seeds[@]}"; do cases+=("adaptive-$seed"); done
  for variant in fixed-conservative fixed-tuned stock-concurrency-adjuster adaptive-throttle-only risk-ordering-only full-method; do
    cases+=("method-$variant")
  done
fi
printf '%s\n' "${cases[@]}" > "$evidence/cases.txt"
if [[ " ${cases[*]} " == *' fixed-ui '* ]]; then
  command -v "$node_bin" >/dev/null
  if [[ "$node_bin" == *.exe ]]; then
    ui_script=$(wslpath -w "$ui_script")
    if [[ "$playwright_module" == /* ]]; then playwright_module=$(wslpath -w "$playwright_module"); fi
  fi
  (cd "$ROOT" && "$node_bin" -e 'require(process.argv[1] || "playwright")' "$playwright_module")
  "$node_bin" --version > "$evidence/node-version.txt"
fi
start_acceptance_runtime "$evidence/runtime"
for label in "${cases[@]}"; do run_case "$label"; done
if [[ -f "$evidence/fixed-cli/result.json" && -f "$evidence/fixed-ui/result.json" ]]; then
  for label in fixed-cli fixed-ui; do
    result=$(jq -r .directory "$evidence/$label/result.json")
    jq -S .spec "$result/experiment.json" > "$evidence/$label/normalized-spec.json"
  done
  diff -u "$evidence/fixed-cli/normalized-spec.json" "$evidence/fixed-ui/normalized-spec.json" > "$evidence/ui-cli.diff"
fi
if [[ -z ${TASK14_CASES:-} ]]; then
  python3 "$check" repeats "$evidence/run-index.jsonl" "$evidence/seed-order.json" > "$evidence/repeats.json"
  python3 "$check" matrix "$evidence/run-index.jsonl" > "$evidence/matrix.json"
fi
echo "PASS acceptance: $evidence"
