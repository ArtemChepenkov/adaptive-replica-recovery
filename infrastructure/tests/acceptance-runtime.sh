#!/usr/bin/env bash
# Source from the acceptance driver, which owns the EXIT trap.

start_acceptance_runtime() {
  local root runtime java runner_jar load_jar cc_jar jar_dir broker_base broker port service attempt
  local cc_address load_address prometheus_address
  [[ $(uname -s) == Linux ]] || { echo 'Run acceptance inside Linux or WSL.' >&2; return 2; }
  root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)
  runtime=$(realpath -m -- "${1:?Expected a new acceptance runtime directory}")
  [[ "$runtime" == "$root"/results/* && ! -e "$runtime" ]] || {
    echo 'Choose a fresh acceptance runtime directory under results/.' >&2
    return 2
  }
  java=${TASK14_JAVA:-java}
  broker_base=${TASK14_BROKER_PORT_BASE:-49092}
  [[ "$broker_base" =~ ^[1-9][0-9]{0,4}$ ]] && (( broker_base >= 1024 && broker_base <= 65530 )) || {
    echo 'TASK14_BROKER_PORT_BASE must leave six valid loopback ports.' >&2
    return 2
  }
  for service in docker curl git jq python3 "$java"; do command -v "$service" >/dev/null || return 2; done
  python3 -c 'import yaml' || return 2
  docker info --format '{{.ServerVersion}}' >/dev/null || return 2
  mkdir -p -- "$runtime/load-output" "$runtime/slo"
  ACCEPTANCE_RUNTIME_DIR=$runtime
  export COMPOSE_PROJECT_NAME="task14-$(date -u +%Y%m%d%H%M%S)-$$"
  export ACCEPTANCE_COMPOSE_OVERRIDE="$runtime/compose.override.yml"
  export MONITORING_PROFILE=final
  export LAB_BROKER_IDS=1,2,3,4,5,6 MONITORING_BROKER_IDS=1,2,3,4,5,6
  export LAB_WAIT_FOR_BROKER_FENCING=true
  export RUNNER_RESULTS_DIRECTORY="$runtime/experiments"
  export RUNNER_PROJECT_DIRECTORY="$root" RUNNER_SCENARIOS_DIRECTORY="$root/infrastructure/scenarios"
  export RUNNER_KAFKA_BOOTSTRAP_SERVERS="127.0.0.1:$broker_base,127.0.0.1:$((broker_base + 1)),127.0.0.1:$((broker_base + 2))"
  printf '%s\n' "$COMPOSE_PROJECT_NAME" > "$runtime/project.txt"
  "$java" -version > "$runtime/java-version.txt" 2>&1
  docker compose version > "$runtime/compose-version.txt"
  docker info --format '{{.ServerVersion}}' > "$runtime/docker-version.txt"
  git -C "$root/cruise-control" rev-parse HEAD > "$runtime/cruise-control-revision.txt"

  runner_jar=${TASK14_RUNNER_JAR:-}
  load_jar=${TASK14_LOAD_JAR:-}
  cc_jar=${TASK14_CRUISE_CONTROL_JAR:-}
  if [[ -z "$runner_jar" || -z "$load_jar" ]]; then
    (cd "$root/experiment-platform" && "$java" -classpath gradle/wrapper/gradle-wrapper.jar \
      org.gradle.wrapper.GradleWrapperMain :experiment-runner:bootJar :load-generator:bootJar \
      --no-daemon --max-workers=2 --console=plain) > "$runtime/platform-build.log" 2>&1 || {
        cat "$runtime/platform-build.log" >&2
        return 1
      }
  fi
  runner_jar=${runner_jar:-$root/experiment-platform/experiment-runner/build/libs/experiment-runner-0.1.0-SNAPSHOT.jar}
  load_jar=${load_jar:-$root/experiment-platform/load-generator/build/libs/load-generator-0.1.0-SNAPSHOT.jar}
  jar_dir=$(mktemp -d /tmp/task14-jars.XXXXXX)
  ACCEPTANCE_JARS_DIR=$jar_dir
  cp -- "$runner_jar" "$jar_dir/runner.jar" || return 1
  cp -- "$load_jar" "$jar_dir/load.jar" || return 1
  if [[ -n "$cc_jar" ]]; then
    mkdir -- "$jar_dir/cruise-jars"
    cp -- "$cc_jar" "$jar_dir/cruise-jars/cruise-control.jar" || return 1
    sha256sum "$jar_dir/cruise-jars/cruise-control.jar" > "$runtime/cruise-control-jar.sha256"
  fi
  sha256sum "$jar_dir/runner.jar" "$jar_dir/load.jar" > "$runtime/platform-jars.sha256"

  # The reporter starts before fixture preparation. Precreate its topic explicitly
  # so it can never add a service-only movement when broker 6 is stopped.
  sed -e 's/\r$//' -e 's/^cruise.control.metrics.topic.auto.create=true$/cruise.control.metrics.topic.auto.create=false/' \
    "$root/infrastructure/kafka/server.properties.template" > "$runtime/server.properties.template"
  grep -qxF 'cruise.control.metrics.topic.auto.create=false' "$runtime/server.properties.template" || return 1
  printf '\nlog.retention.check.interval.ms=10000\n' >> "$runtime/server.properties.template"
  cat "$root/infrastructure/cruise-control/cruisecontrol.properties" \
    "$root/cruise-control/config/recovery-risk.properties" > "$runtime/cruisecontrol.properties"
  cat >> "$runtime/cruisecontrol.properties" <<'CONFIG'
default.goals=com.linkedin.kafka.cruisecontrol.analyzer.goals.RackAwareDistributionGoal,com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaDistributionGoal
hard.goals=com.linkedin.kafka.cruisecontrol.analyzer.goals.RackAwareDistributionGoal
anomaly.detection.goals=com.linkedin.kafka.cruisecontrol.analyzer.goals.RackAwareDistributionGoal
self.healing.goals=com.linkedin.kafka.cruisecontrol.analyzer.goals.RackAwareDistributionGoal
self.healing.enabled=false
partition.metrics.window.ms=30000
broker.metrics.window.ms=30000
num.partition.metrics.windows=2
num.broker.metrics.windows=2
metadata.max.age.ms=1000
execution.progress.check.interval.ms=5000
partition.sample.store.topic.partition.count=1
broker.sample.store.topic.partition.count=1
concurrency.adjuster.inter.broker.replica.enabled=false
concurrency.adjuster.interval.ms=10000
concurrency.adjuster.min.isr.check.enabled=false
max.active.user.tasks=30
recovery.control.enabled=true
recovery.control.slo.url=http://slo-proxy:8082/api/load/metrics/current
recovery.control.safe.fallback.throttle.bytes.per.second=1048576
CONFIG

  # Only Cruise Control's path goes through this proxy. Runner and the load
  # generator keep exchanging real measurements during the injected outage.
  cat > "$runtime/slo/proxy.py" <<'PYTHON'
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.error import URLError
from urllib.request import urlopen

PATH = '/api/load/metrics/current'

class Proxy(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path != PATH:
            self.send_error(404)
            return
        if Path('/slo/unavailable').exists():
            self.send_error(503, 'SLO link interrupted by acceptance')
            return
        try:
            with urlopen('http://load-generator:8081' + PATH, timeout=2) as upstream:
                body = upstream.read()
        except (URLError, TimeoutError):
            self.send_error(503, 'Load generator unavailable')
            return
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

ThreadingHTTPServer(('0.0.0.0', 8082), Proxy).serve_forever()
PYTHON

  {
    echo 'services:'
    for broker in {1..6}; do
      port=$((broker_base + broker - 1))
      cat <<YAML
  broker-$broker:
    ports: !override ["127.0.0.1:$port:9094"]
    environment: {EXTERNAL_PORT: "$port"}
    volumes:
      - "$runtime/server.properties.template:/etc/recovery/server.properties.template:ro"
YAML
    done
    cat <<YAML
  cruise-control:
    ports: !override ["127.0.0.1::9090"]
    volumes:
      - "$runtime/cruisecontrol.properties:/opt/cruise-control/config/cruisecontrol.properties:ro"
YAML
    if [[ -n "$cc_jar" ]]; then
      echo "      - \"$jar_dir/cruise-jars:/opt/cruise-control/cruise-control/build/libs:ro\""
    fi
    cat <<YAML
  prometheus:
    ports: !override ["127.0.0.1::9090"]
  load-generator:
    image: eclipse-temurin:17-jre-jammy
    entrypoint: ["java", "-Xms64m", "-Xmx256m", "-XX:ActiveProcessorCount=2", "-jar", "/app.jar"]
    mem_limit: 768m
    cpus: 1.0
    ports: ["127.0.0.1::8081"]
    environment:
      SERVER_ADDRESS: 0.0.0.0
      KAFKA_BOOTSTRAP_SERVERS: broker-1:9092,broker-2:9092,broker-3:9092
      KAFKA_LOAD_TOPIC: acceptance-load
      LOAD_OUTPUT_DIRECTORY: /load-output
    volumes:
      - "$jar_dir/load.jar:/app.jar:ro"
      - "$runtime/load-output:/load-output"
    networks: [backbone]
  slo-proxy:
    image: replica-recovery-kafka:4.0.0
    entrypoint: ["python3", "-u", "/slo/proxy.py"]
    mem_limit: 64m
    volumes:
      - "$runtime/slo:/slo:ro"
    networks: [backbone]
networks:
  zone-a: {name: "$COMPOSE_PROJECT_NAME-zone-a"}
  zone-b: {name: "$COMPOSE_PROJECT_NAME-zone-b"}
  zone-c: {name: "$COMPOSE_PROJECT_NAME-zone-c"}
YAML
  } > "$ACCEPTANCE_COMPOSE_OVERRIDE"

  compose=(docker compose --profile final -p "$COMPOSE_PROJECT_NAME"
    -f "$root/infrastructure/docker-compose.yml" -f "$ACCEPTANCE_COMPOSE_OVERRIDE")
  "${compose[@]}" config --quiet || return 1
  if ! docker image inspect replica-recovery-kafka:4.0.0 >/dev/null 2>&1; then
    "${compose[@]}" build broker-1 >> "$runtime/compose-build.log" 2>&1 || return 1
  fi
  if [[ -z "$cc_jar" ]] || ! docker image inspect replica-recovery-cruise-control:baseline >/dev/null 2>&1; then
    "${compose[@]}" build cruise-control >> "$runtime/compose-build.log" 2>&1 || return 1
  fi
  "${compose[@]}" up -d --no-build --wait --wait-timeout 300 broker-{1..6} || return 1
  for service in __CruiseControlMetrics __KafkaCruiseControlPartitionMetricSamples __KafkaCruiseControlModelTrainingSamples; do
    "${compose[@]}" exec -T -e KAFKA_HEAP_OPTS=-Xmx128m broker-1 /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server broker-1:9092 --create --if-not-exists --topic "$service" \
      --replica-assignment 1:2:3 --config min.insync.replicas=2 >> "$runtime/bootstrap-topics.log" 2>&1 || return 1
  done
  "${compose[@]}" exec -T -e KAFKA_HEAP_OPTS=-Xmx128m broker-1 /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server broker-1:9092 --create --if-not-exists --topic acceptance-load \
    --replica-assignment 1:2:3,2:3:1,3:1:2,1:2:3,2:3:1,3:1:2,1:2:3,2:3:1,3:1:2,1:2:3,2:3:1,3:1:2 \
    --config min.insync.replicas=2 --config retention.bytes=8388608 --config segment.bytes=2097152 \
    >> "$runtime/bootstrap-topics.log" 2>&1 || return 1

  "${compose[@]}" up -d --no-build --wait --wait-timeout 300 cruise-control prometheus load-generator slo-proxy || return 1
  docker image inspect --format '{{.Id}} {{json .RepoTags}}' replica-recovery-kafka:4.0.0 \
    replica-recovery-cruise-control:baseline eclipse-temurin:17-jre-jammy prom/prometheus:v3.5.1 \
    > "$runtime/images.txt" || return 1
  cc_address=$("${compose[@]}" port cruise-control 9090) || return 1
  prometheus_address=$("${compose[@]}" port prometheus 9090) || return 1
  load_address=$("${compose[@]}" port load-generator 8081) || return 1
  export CRUISE_CONTROL_URL="http://$cc_address" PROMETHEUS_URL="http://$prometheus_address"
  export LOAD_API_URL="http://$load_address"
  export LOAD_GENERATOR_URL="$LOAD_API_URL"
  cc="$CRUISE_CONTROL_URL/kafkacruisecontrol"
  load="$LOAD_API_URL/api/load"
  acceptance_wait_http "$load/status" "$runtime/load-status.json" || return 1
  acceptance_wait_http "$cc/state?json=true" "$runtime/cruise-state.json" || return 1
  acceptance_wait_http "$PROMETHEUS_URL/-/ready" "$runtime/prometheus-ready.txt" || return 1
  "${compose[@]}" exec -T slo-proxy curl -fsS --max-time 8 http://localhost:8082/api/load/metrics/current \
    > "$runtime/slo-proxy-status.json" || return 1

  : > "$runtime/runner.log"
  (
    # Java needs a native current directory even when artifacts are on a Windows mount.
    cd /tmp
    RUNNER_HTTP_PORT=0 SPRING_PROFILES_ACTIVE= RUNNER_POLL_INTERVAL="${TASK14_RUNNER_POLL_INTERVAL:-2s}" \
      exec "$java" -Xms64m -Xmx384m -XX:ActiveProcessorCount=2 -jar "$jar_dir/runner.jar"
  ) > "$runtime/runner.log" 2>&1 &
  ACCEPTANCE_RUNNER_PID=$!
  for attempt in {1..60}; do
    kill -0 "$ACCEPTANCE_RUNNER_PID" 2>/dev/null || { cat "$runtime/runner.log" >&2; return 1; }
    port=$(sed -n 's/.*Tomcat started on port \([0-9][0-9]*\).*/\1/p' "$runtime/runner.log" | tail -1)
    [[ -z "$port" ]] || break
    sleep 1
  done
  [[ -n "$port" ]] || { cat "$runtime/runner.log" >&2; return 1; }
  export RUNNER_API_URL="http://127.0.0.1:$port"
  acceptance_wait_http "$RUNNER_API_URL/api/profiles" "$runtime/runner-profiles.json" || return 1
  printf '%s\n' "$RUNNER_API_URL" "$LOAD_API_URL" "$CRUISE_CONTROL_URL" "$PROMETHEUS_URL" > "$runtime/urls.txt"
}

acceptance_wait_http() {
  local url=$1 output=$2 attempt
  for attempt in {1..45}; do
    if curl -fsS --max-time 3 "$url" > "$output" 2>/dev/null; then return 0; fi
    sleep 2
  done
  echo "Timed out waiting for $url" >&2
  return 1
}

acceptance_slo_unavailable() {
  date -u +%FT%TZ > "${ACCEPTANCE_RUNTIME_DIR:?Runtime has not started}/slo/unavailable"
}

acceptance_slo_available() {
  rm -f -- "${ACCEPTANCE_RUNTIME_DIR:?Runtime has not started}/slo/unavailable"
}

stop_acceptance_runtime() {
  local rc=0 attempt
  if [[ -n ${ACCEPTANCE_RUNNER_PID:-} ]]; then
    if jobs -pr | grep -qxF "$ACCEPTANCE_RUNNER_PID"; then
      kill "$ACCEPTANCE_RUNNER_PID" 2>/dev/null || true
    fi
    for attempt in {1..10}; do
      jobs -pr | grep -qxF "$ACCEPTANCE_RUNNER_PID" || break
      sleep 1
    done
    if jobs -pr | grep -qxF "$ACCEPTANCE_RUNNER_PID"; then
      kill -KILL "$ACCEPTANCE_RUNNER_PID" 2>/dev/null || true
    fi
    wait "$ACCEPTANCE_RUNNER_PID" 2>/dev/null || true
    unset ACCEPTANCE_RUNNER_PID
  fi
  if [[ ${COMPOSE_PROJECT_NAME:-} =~ ^task14-[0-9]{14}-[0-9]+$ && -f ${ACCEPTANCE_COMPOSE_OVERRIDE:-} ]]; then
    "${compose[@]}" logs --no-color > "$ACCEPTANCE_RUNTIME_DIR/compose.log" 2>&1 || rc=1
    "${compose[@]}" down --volumes --timeout 15 > "$ACCEPTANCE_RUNTIME_DIR/compose-down.log" 2>&1 || rc=1
    {
      docker ps -aq --filter "label=com.docker.compose.project=$COMPOSE_PROJECT_NAME" || rc=1
      docker network ls -q --filter "label=com.docker.compose.project=$COMPOSE_PROJECT_NAME" || rc=1
      docker volume ls -q --filter "label=com.docker.compose.project=$COMPOSE_PROJECT_NAME" || rc=1
    } > "$ACCEPTANCE_RUNTIME_DIR/remaining-docker-resources.txt"
    [[ ! -s "$ACCEPTANCE_RUNTIME_DIR/remaining-docker-resources.txt" ]] || rc=1
  fi
  if [[ "$rc" -eq 0 && ${ACCEPTANCE_JARS_DIR:-} =~ ^/tmp/task14-jars\.[a-zA-Z0-9]{6}$ ]]; then
    rm -f -- "$ACCEPTANCE_JARS_DIR/runner.jar" "$ACCEPTANCE_JARS_DIR/load.jar" "$ACCEPTANCE_JARS_DIR/cruise-jars/cruise-control.jar" || rc=1
    if [[ -d "$ACCEPTANCE_JARS_DIR/cruise-jars" ]]; then rmdir -- "$ACCEPTANCE_JARS_DIR/cruise-jars" || rc=1; fi
    rmdir -- "$ACCEPTANCE_JARS_DIR" || rc=1
  fi
  return "$rc"
}
