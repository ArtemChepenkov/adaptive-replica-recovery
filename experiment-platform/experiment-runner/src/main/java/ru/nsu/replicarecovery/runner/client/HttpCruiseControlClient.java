package ru.nsu.replicarecovery.runner.client;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import ru.nsu.replicarecovery.model.ExperimentSpec;
import ru.nsu.replicarecovery.model.RecoveryExecutionSpec;
import ru.nsu.replicarecovery.model.RecoverySpec;

/** An execution belongs to one tracked User-Task-ID; HTTP acceptance is not execution completion. */
public final class HttpCruiseControlClient implements CruiseControlClient, AutoCloseable {
  private static final long MIB = 1_048_576L;
  private static final String INTER_BROKER_REPLICA = "INTER_BROKER_REPLICA";
  private final HttpJsonTransport http;
  private final KafkaRecoveryTelemetry kafka;
  private String taskId;
  private boolean ambiguousStart;
  private long lastThrottle;
  private RecoverySpec recovery;
  private Long previousFinishedBytes;
  private long previousFinishedAt;
  private Boolean previousAdjuster;
  private boolean adjusterUnverified;

  public HttpCruiseControlClient(String baseUrl) { this(baseUrl, null); }
  public HttpCruiseControlClient(String baseUrl, String bootstrapServers) {
    // Cruise Control waits up to ten seconds by default before returning async progress and the task ID.
    this(new HttpJsonTransport(baseUrl, Duration.ofSeconds(15)), bootstrapServers == null || bootstrapServers.isBlank()
        ? null : new KafkaRecoveryTelemetry(bootstrapServers));
  }
  HttpCruiseControlClient(HttpJsonTransport http) { this(http, null); }
  private HttpCruiseControlClient(HttpJsonTransport http, KafkaRecoveryTelemetry kafka) {
    this.http = http;
    this.kafka = kafka;
  }

  @Override
  public synchronized String startRecovery(ExperimentSpec spec) {
    if (previousAdjuster != null || adjusterUnverified) {
      throw new IllegalStateException("previous concurrency adjuster setting has not been restored");
    }
    taskId = null;
    ambiguousStart = false;
    previousFinishedBytes = null;
    recovery = spec.recovery();
    lastThrottle = Math.multiplyExact(recovery.initialThrottleMiBps(), MIB);
    String query = recoveryRequest(recovery);
    if (recovery.execution() != null && recovery.execution().stockConcurrencyAdjusterEnabled() != null) {
      configureAdjuster(recovery.execution().stockConcurrencyAdjusterEnabled());
    }
    ambiguousStart = true;
    HttpJsonTransport.Reply reply;
    try {
      reply = http.post(query, null);
    } catch (RemoteCallException exception) {
      // A definite client error rejected this request. Timeouts and server errors
      // cannot prove whether the execution was accepted before the response failed.
      if (definiteRejection(exception)) {
        ambiguousStart = false;
      }
      throw exception;
    }
    String id = reply.headers().firstValue("User-Task-ID").orElse("").trim();
    if (id.isEmpty()) {
      throw new RemoteCallException("ambiguous Cruise Control start: User-Task-ID header missing", 200);
    }
    taskId = id;
    ambiguousStart = false;
    return id;
  }

  private String recoveryRequest(RecoverySpec spec) {
    RecoveryExecutionSpec execution = spec.execution();
    boolean remove = execution != null && execution.operation() == RecoveryExecutionSpec.Operation.REMOVE_BROKER;
    StringBuilder query = new StringBuilder("/kafkacruisecontrol/")
        .append(remove ? "remove_broker" : "rebalance")
        .append("?dryrun=false&json=true&replication_throttle=").append(lastThrottle);
    if (remove) {
      query.append("&brokerid=").append(execution.brokerId());
    } else {
      query.append("&recovery_control=").append(spec.adaptiveThrottleEnabled());
    }
    if (spec.riskOrderingEnabled()) {
      query.append("&replica_movement_strategies=RiskAwareReplicaMovementStrategy");
    }
    if (execution != null) {
      if (execution.goal() != null) {
        query.append("&goals=").append(HttpJsonTransport.query(execution.goal())).append("&skip_hard_goal_check=true");
      }
      if (execution.excludedTopics() != null) {
        query.append("&excluded_topics=").append(HttpJsonTransport.query(execution.excludedTopics()));
      }
      if (execution.concurrentMovements() != null) {
        query.append("&concurrent_partition_movements_per_broker=").append(execution.concurrentMovements());
      }
      if (execution.maxClusterMovements() != null) {
        query.append("&max_partition_movements_in_cluster=").append(execution.maxClusterMovements());
      }
      if (execution.destinationBrokerIds() != null) {
        query.append("&destination_broker_ids=").append(execution.destinationBrokerIds().stream()
            .map(String::valueOf).collect(Collectors.joining(",")));
      }
    }
    return query.toString();
  }

  private void configureAdjuster(boolean enabled) {
    adjusterUnverified = true;
    JsonNode response;
    try {
      response = setAdjuster(enabled);
    } catch (RemoteCallException exception) {
      if (definiteRejection(exception)) adjusterUnverified = false;
      throw exception;
    }
    previousAdjuster = adjusterState(response, "concurrencyAdjusterEnabledBefore");
    adjusterUnverified = false;
    verifyAdjuster(response, enabled);
  }

  @Override
  public synchronized void restoreRecoverySettings() {
    if (adjusterUnverified) {
      throw new IllegalStateException("previous concurrency adjuster setting is unknown");
    }
    if (previousAdjuster != null) {
      verifyAdjuster(setAdjuster(previousAdjuster), previousAdjuster);
      previousAdjuster = null;
    }
  }

  private JsonNode setAdjuster(boolean enabled) {
    return http.post("/kafkacruisecontrol/admin?json=true&"
        + (enabled ? "enable" : "disable") + "_concurrency_adjuster_for=" + INTER_BROKER_REPLICA, null).body();
  }

  private static void verifyAdjuster(JsonNode response, boolean enabled) {
    if (adjusterState(response, "concurrencyAdjusterEnabledAfter") != enabled) {
      throw new RemoteCallException("Cruise Control did not apply the requested concurrency adjuster state", 200);
    }
  }

  private static boolean adjusterState(JsonNode response, String key) {
    JsonNode state = HttpJsonTransport.required(HttpJsonTransport.required(response, key), INTER_BROKER_REPLICA);
    if (!state.isBoolean()) throw new RemoteCallException("invalid concurrency adjuster state: " + key, 200);
    return state.booleanValue();
  }

  private static boolean definiteRejection(RemoteCallException exception) {
    return exception.statusCode() >= 400 && exception.statusCode() < 500 && exception.statusCode() != 408;
  }

  @Override
  public synchronized RecoveryControlStatus status() {
    if (ambiguousStart) {
      throw new RemoteCallException("ambiguous Cruise Control start; execution ownership cannot be verified", 0);
    }
    JsonNode state = http.get("/kafkacruisecontrol/state?json=true");
    JsonNode execution = HttpJsonTransport.required(state, "ExecutorState");
    String executor = HttpJsonTransport.text(execution, "state");
    boolean idle = executor.equals("NO_TASK_IN_PROGRESS");
    if (taskId == null) {
      // The executor can still be idle while an accepted POST is calculating
      // its proposal. Such a task can start moving replicas after this read.
      boolean pendingMutation = hasUntrackedMutation();
      boolean active = !idle || pendingMutation;
      return new RecoveryControlStatus(false, false, active, lastThrottle, null, null, null, null,
          active ? "UNTRACKED_EXECUTION" : "NO_TRACKED_EXECUTION", Map.of());
    }
    JsonNode tasks = HttpJsonTransport.required(http.get("/kafkacruisecontrol/user_tasks?json=true&user_task_ids="
        + HttpJsonTransport.query(taskId)), "userTasks");
    if (!tasks.isArray()) {
      throw new RemoteCallException("invalid Cruise Control userTasks", 200);
    }
    String taskState = null;
    for (JsonNode task : tasks) {
      if (taskId.equals(task.path("UserTaskId").asText())) {
        if (taskState != null) {
          throw new RemoteCallException("duplicate tracked User-Task-ID", 200);
        }
        taskState = HttpJsonTransport.text(task, "Status");
      }
    }
    if (taskState == null || !(taskState.equals("Active") || taskState.equals("InExecution")
        || taskState.equals("Completed") || taskState.equals("CompletedWithError"))) {
      throw new RemoteCallException("tracked Cruise Control task absent or has an unknown state", 200);
    }
    boolean terminal = taskState.equals("Completed") || taskState.equals("CompletedWithError");
    boolean failed = taskState.equals("CompletedWithError");
    boolean completed = taskState.equals("Completed") && idle;
    JsonNode control;
    try {
      control = http.get("/kafkacruisecontrol/recovery_control?json=true");
    } catch (RemoteCallException exception) {
      if (exception.statusCode() != 404) { throw exception; }
      control = http.json.createObjectNode();
    }
    if (control.hasNonNull("executionId") && !taskId.equals(control.path("executionId").asText())) {
      control = http.json.createObjectNode();
    }
    Long throttle = optionalLong(control, "currentThrottleBytesPerSecond");
    if (throttle != null) lastThrottle = throttle;
    Long remaining = optionalLong(control, "remainingBytes");
    Long progress = optionalLong(control, "progressBytesPerSecond");
    if ((remaining == null || progress == null) && taskId.equals(execution.path("triggeredUserTaskId").asText())) {
      Long totalMiB = optionalLong(execution, "totalDataToMove");
      Long finishedMiB = optionalLong(execution, "finishedDataMovement");
      if (totalMiB != null && finishedMiB != null && totalMiB >= finishedMiB) {
        long observedAt = System.nanoTime();
        long finishedBytes = Math.multiplyExact(finishedMiB, MIB);
        if (remaining == null) remaining = Math.multiplyExact(totalMiB - finishedMiB, MIB);
        if (progress == null && previousFinishedBytes != null && finishedBytes >= previousFinishedBytes
            && observedAt > previousFinishedAt) {
          progress = Math.round((finishedBytes - previousFinishedBytes) * 1_000_000_000.0 / (observedAt - previousFinishedAt));
        }
        previousFinishedBytes = finishedBytes;
        previousFinishedAt = observedAt;
      }
    }
    Long critical = optionalLong(control, "criticalPartitions");
    if (critical != null && critical > Integer.MAX_VALUE) {
      throw new RemoteCallException("criticalPartitions exceeds integer range", 200);
    }
    Boolean safe = null;
    if (control.hasNonNull("safeState")) {
      if (!control.get("safeState").isBoolean()) {
        throw new RemoteCallException("invalid safeState", 200);
      }
      safe = control.get("safeState").booleanValue();
    }
    if (kafka != null && (critical == null || safe == null)) {
      var observation = kafka.current(recovery);
      if (observation != null) {
        if (critical == null) critical = (long) observation.criticalPartitions();
        if (safe == null) safe = observation.safe();
      }
    }
    String reason = control.hasNonNull("reason")
        ? HttpJsonTransport.text(control, "reason") : "RECOVERY_TELEMETRY_UNAVAILABLE";
    Map<String, Object> decision = new LinkedHashMap<>();
    for (String field : new String[] {"reason", "lastAction", "constraintState", "decisionTimestampMs"}) {
      JsonNode value = control.get(field);
      if (value != null && !value.isNull()) {
        decision.put(field, field.equals("decisionTimestampMs")
            ? HttpJsonTransport.nonnegativeLong(control, field) : HttpJsonTransport.text(control, field));
      }
    }
    if (taskId.equals(execution.path("triggeredUserTaskId").asText())) {
      Long concurrency = optionalLong(execution, "maximumConcurrentPartitionMovementsPerBroker");
      if (concurrency != null) decision.put("maxConcurrentMovementsPerBroker", concurrency);
    }
    if (!decision.isEmpty()) {
      decision.putIfAbsent("reason", reason);
      decision.put("throttleBytesPerSecond", lastThrottle);
    }
    return new RecoveryControlStatus(completed, failed, !(terminal && idle),
        lastThrottle, remaining, progress,
        critical == null ? null : critical.intValue(), safe, reason, decision);
  }

  @Override
  public synchronized void stopRecovery() {
    http.post("/kafkacruisecontrol/stop_proposal_execution?json=true", null);
  }

  @Override public void close() { if (kafka != null) kafka.close(); }

  private static Long optionalLong(JsonNode node, String field) {
    return node.hasNonNull(field) ? HttpJsonTransport.nonnegativeLong(node, field) : null;
  }

  private boolean hasUntrackedMutation() {
    JsonNode tasks = HttpJsonTransport.required(http.get("/kafkacruisecontrol/user_tasks?json=true"), "userTasks");
    if (!tasks.isArray()) {
      throw new RemoteCallException("invalid Cruise Control userTasks", 200);
    }
    boolean mutation = false;
    for (JsonNode task : tasks) {
      String status = HttpJsonTransport.text(task, "Status");
      if (status.equals("Completed") || status.equals("CompletedWithError")) {
        continue;
      }
      if (!status.equals("Active") && !status.equals("InExecution")) {
        throw new RemoteCallException("unknown Cruise Control task state", 200);
      }
      String request = HttpJsonTransport.text(task, "RequestURL");
      String[] parts = request.trim().split("\\s+", 2);
      if (parts.length != 2 || parts[1].isBlank() || !parts[0].matches("[A-Za-z]+")) {
        throw new RemoteCallException("invalid active task RequestURL", 200);
      }
      String method = parts[0].toUpperCase(Locale.ROOT);
      if (!(method.equals("GET") || method.equals("HEAD") || method.equals("OPTIONS"))) {
        mutation = true;
      }
    }
    return mutation;
  }
}
