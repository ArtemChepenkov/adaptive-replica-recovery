package ru.nsu.replicarecovery.runner.metrics;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.function.Supplier;
import ru.nsu.replicarecovery.runner.client.HttpJsonTransport;

/** Read-only state lookup, separate from ownership/tracking of a recovery task. */
final class CruiseControlMonitoringSource implements Supplier<String> {
  private final HttpJsonTransport http;
  CruiseControlMonitoringSource(String url) { http = HttpJsonTransport.monitoring(url); }
  public String get() {
    JsonNode state = http.get("/kafkacruisecontrol/state?json=true").path("ExecutorState").path("state");
    if (!state.isTextual() || state.textValue().isBlank()) throw new IllegalStateException("executor state unavailable");
    return state.textValue();
  }
}
