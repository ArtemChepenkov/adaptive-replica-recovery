package ru.nsu.replicarecovery.runner.client;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import ru.nsu.replicarecovery.model.ExperimentSpec;
import ru.nsu.replicarecovery.model.ExperimentSpecValidator;
import ru.nsu.replicarecovery.model.LoadMetricsSnapshot;

public final class HttpLoadGeneratorClient implements LoadGeneratorClient {
  private static final Set<String> STATES = Set.of("IDLE", "STARTING", "RUNNING", "STOPPING", "STOPPED", "COMPLETED", "FAILED");
  private final HttpJsonTransport http;

  public HttpLoadGeneratorClient(String baseUrl) { this(new HttpJsonTransport(baseUrl)); }
  HttpLoadGeneratorClient(HttpJsonTransport http) { this.http = http; }

  @Override
  public LoadStatus start(ExperimentSpec spec) {
    return decodeStatus(http.post("/api/load/start", Map.of("load", spec.load(), "slo", spec.slo(),
        "recovery", spec.recovery(), "seed", spec.seed())).body());
  }

  @Override public LoadStatus status() { return decodeStatus(http.get("/api/load/status")); }
  @Override public LoadStatus stop() { return decodeStatus(http.post("/api/load/stop", null).body()); }

  @Override
  public LoadMetricsSnapshot metrics() {
    JsonNode node = http.get("/api/load/metrics/current");
    for (String field : new String[] {"windowStart", "windowEnd", "generatedAt", "requestCount", "errorCount",
        "errorRate", "offeredRps", "achievedRps", "bytesSent", "outstandingRequests"}) {
      HttpJsonTransport.required(node, field);
    }
    try {
      LoadMetricsSnapshot value = http.json.treeToValue(node, LoadMetricsSnapshot.class);
      if (value.windowStart() == null || value.windowEnd() == null || value.generatedAt() == null
          || !value.windowEnd().isAfter(value.windowStart()) || value.generatedAt().isBefore(value.windowEnd())
          || value.requestCount() < 0 || value.errorCount() < 0 || value.errorCount() > value.requestCount()
          || !finite(value.errorRate()) || value.errorRate() > 1 || !finite(value.offeredRps())
          || !finite(value.achievedRps()) || value.bytesSent() < 0 || value.outstandingRequests() < 0) {
        throw new IllegalArgumentException("invalid metric counts, timing or rates");
      }
      double expectedErrorRate = value.requestCount() == 0 ? 0 : (double) value.errorCount() / value.requestCount();
      if (Math.abs(expectedErrorRate - value.errorRate()) > 1e-9) {
        throw new IllegalArgumentException("errorRate does not match counts");
      }
      boolean successes = value.requestCount() > value.errorCount();
      if (!percentile(value.p50Ms(), successes) || !percentile(value.p95Ms(), successes)
          || !percentile(value.p99Ms(), successes)
          || (successes && (value.p50Ms() > value.p95Ms() || value.p95Ms() > value.p99Ms()))) {
        throw new IllegalArgumentException("invalid latency percentiles");
      }
      if (value.controlSettings() != null) {
        ExperimentSpecValidator.validateSlo(value.controlSettings().slo());
        ExperimentSpecValidator.validateRecovery(value.controlSettings().recovery());
      } else if (value.requestCount() > 0) {
        throw new IllegalArgumentException("controlSettings missing from active metrics");
      }
      return value;
    } catch (IOException | IllegalArgumentException exception) {
      throw new RemoteCallException("invalid Load Generator metrics", exception);
    }
  }

  private LoadStatus decodeStatus(JsonNode node) {
    String state = HttpJsonTransport.text(node, "state");
    long outstanding = HttpJsonTransport.nonnegativeLong(node, "outstandingRequests");
    JsonNode cleanup = HttpJsonTransport.required(node, "cleanupComplete");
    if (!STATES.contains(state) || !cleanup.isBoolean()
        || (cleanup.booleanValue() && outstanding != 0)) {
      throw new RemoteCallException("invalid Load Generator status", 200);
    }
    String runId = node.path("runId").isNull() || node.path("runId").isMissingNode()
        ? null : HttpJsonTransport.text(node, "runId");
    if (!state.equals("IDLE") && runId == null) {
      throw new RemoteCallException("Load Generator runId missing", 200);
    }
    String error = node.path("error").isTextual() ? node.path("error").textValue() : null;
    return new LoadStatus(state, runId, cleanup.booleanValue(), outstanding, error);
  }

  private static boolean finite(double value) { return Double.isFinite(value) && value >= 0; }
  private static boolean percentile(Double value, boolean successes) {
    return successes ? value != null && finite(value) : value == null;
  }
}
