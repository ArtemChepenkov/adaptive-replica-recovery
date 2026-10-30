package ru.nsu.replicarecovery.runner.client;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import ru.nsu.replicarecovery.model.ExperimentSpec;

/** An execution belongs to one tracked User-Task-ID; HTTP acceptance is not execution completion. */
public final class HttpCruiseControlClient implements CruiseControlClient {
  private final HttpJsonTransport http;
  private volatile String taskId;
  private volatile boolean ambiguousStart;
  private volatile long initialThrottle;

  public HttpCruiseControlClient(String baseUrl) { this(new HttpJsonTransport(baseUrl)); }
  HttpCruiseControlClient(HttpJsonTransport http) { this.http = http; }

  @Override
  public synchronized String startRecovery(ExperimentSpec spec) {
    initialThrottle = Math.multiplyExact(spec.recovery().initialThrottleMiBps(), 1_048_576L);
    String query = "/kafkacruisecontrol/rebalance?dryrun=false&json=true&replication_throttle=" + initialThrottle
        + "&recovery_control=" + spec.recovery().adaptiveThrottleEnabled();
    if (spec.recovery().riskOrderingEnabled()) {
      query += "&replica_movement_strategies=" + HttpJsonTransport.query("RiskAwareReplicaMovementStrategy");
    }
    taskId = null;
    ambiguousStart = true;
    HttpJsonTransport.Reply reply;
    try {
      reply = http.post(query, null);
    } catch (RemoteCallException exception) {
      // A definite client error rejected this request. Timeouts and server errors
      // cannot prove whether the execution was accepted before the response failed.
      if (exception.statusCode() >= 400 && exception.statusCode() < 500
          && exception.statusCode() != 408) {
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

  @Override
  public synchronized RecoveryControlStatus status() {
    if (ambiguousStart) {
      throw new RemoteCallException("ambiguous Cruise Control start; execution ownership cannot be verified", 0);
    }
    JsonNode state = http.get("/kafkacruisecontrol/state?json=true");
    String executor = HttpJsonTransport.text(HttpJsonTransport.required(state, "ExecutorState"), "state");
    boolean idle = executor.equals("NO_TASK_IN_PROGRESS");
    if (taskId == null) {
      // The executor can still be idle while an accepted POST is calculating
      // its proposal. Such a task can start moving replicas after this read.
      boolean pendingMutation = hasUntrackedMutation();
      boolean active = !idle || pendingMutation;
      return new RecoveryControlStatus(false, false, active, initialThrottle, null, null, null, null,
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
    Long throttle = optionalLong(control, "currentThrottleBytesPerSecond");
    Long remaining = optionalLong(control, "remainingBytes");
    Long progress = optionalLong(control, "progressBytesPerSecond");
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
    return new RecoveryControlStatus(completed, failed, !(terminal && idle),
        throttle == null ? initialThrottle : throttle, remaining, progress,
        critical == null ? null : critical.intValue(), safe, reason, decision);
  }

  @Override
  public synchronized void stopRecovery() {
    http.post("/kafkacruisecontrol/stop_proposal_execution?json=true", null);
  }

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
