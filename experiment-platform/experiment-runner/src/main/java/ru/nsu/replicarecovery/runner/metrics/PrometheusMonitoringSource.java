package ru.nsu.replicarecovery.runner.metrics;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Clock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import ru.nsu.replicarecovery.runner.client.HttpJsonTransport;

/** A single range-vector request retains real scrape timestamps for freshness checks. */
final class PrometheusMonitoringSource implements Supplier<PrometheusMonitoringSource.Reading> {
  private static final String QUERY = "{__name__=~\"broker_network_out_bytes_total|broker_under_replicated_partitions|cruise_control_executor_replica_actions|up\",job=~\"kafka|cruise-control\"}[15s]";
  private static final Set<String> ACTIONS = Set.of("replica-action/in-progress", "replica-action/aborting",
      "intra-broker-replica-action/in-progress", "intra-broker-replica-action/aborting");
  private final HttpJsonTransport http;
  private final Set<Integer> expected;
  private final Clock clock;
  PrometheusMonitoringSource(String url, Set<Integer> expected, Clock clock) {
    this.http = HttpJsonTransport.monitoring(url); this.expected = Set.copyOf(expected); this.clock = clock;
  }

  public Reading get() {
    JsonNode response = http.get("/api/v1/query?query=" + HttpJsonTransport.query(QUERY));
    JsonNode data = response.path("data");
    if (!response.path("status").asText().equals("success")
        || !data.path("resultType").asText().equals("matrix") || !data.path("result").isArray()) {
      throw new IllegalStateException("Prometheus range vector required");
    }
    Map<Integer, Double> network = new HashMap<>();
    Map<Integer, Double> urp = new HashMap<>();
    Map<String, Double> actions = new HashMap<>();
    Map<Integer, Double> brokerUp = new HashMap<>();
    Double cruiseUp = null;
    Set<String> seen = new HashSet<>();
    List<String> errors = new ArrayList<>();
    for (JsonNode series : data.path("result")) {
      JsonNode labels = series.path("metric");
      String name = labels.path("__name__").asText();
      try {
        if (name.equals("up")) {
          if (labels.path("job").asText().equals("cruise-control")) {
            if (!seen.add("cruise-up")) { cruiseUp = null; throw new IllegalArgumentException("duplicate target"); }
            cruiseUp = latest(series.path("values")).value();
          } else if (labels.path("job").asText().equals("kafka")) {
            int broker = Integer.parseInt(labels.path("broker_id").asText());
            if (!expected.contains(broker)) continue;
            if (!seen.add("up/" + broker)) { brokerUp.remove(broker); throw new IllegalArgumentException("duplicate target"); }
            brokerUp.put(broker, latest(series.path("values")).value());
          }
        } else if (name.equals("cruise_control_executor_replica_actions")) {
          String key = labels.path("type").asText() + "/" + labels.path("state").asText();
          if (!ACTIONS.contains(key)) continue;
          if (!seen.add(name + key)) { actions.remove(key); throw new IllegalArgumentException("duplicate"); }
          actions.put(key, latest(series.path("values")).value());
        } else if (name.equals("broker_network_out_bytes_total") || name.equals("broker_under_replicated_partitions")) {
          String label = labels.path("broker_id").asText();
          if (!label.matches("[1-9][0-9]*")) throw new IllegalArgumentException("broker label required");
          int broker = Integer.parseInt(label);
          if (!expected.contains(broker)) continue;
          Map<Integer, Double> destination = name.equals("broker_network_out_bytes_total") ? network : urp;
          if (!seen.add(name + "/" + broker)) { destination.remove(broker); throw new IllegalArgumentException("duplicate"); }
          JsonNode samples = series.path("values");
          Sample last = latest(samples);
          double value = last.value();
          if (destination == network) {
            if (samples.size() < 2) throw new IllegalArgumentException("two counter samples required");
            Sample before = sample(samples.get(samples.size() - 2));
            double delta = last.value() - before.value();
            if (delta < 0 || last.time() <= before.time()) throw new IllegalArgumentException("counter reset");
            value = delta / (last.time() - before.time());
            if (!Double.isFinite(value)) throw new IllegalArgumentException("invalid counter rate");
          }
          destination.put(broker, value);
        }
      } catch (RuntimeException exception) {
        errors.add("INVALID_PROMETHEUS_SERIES:" + name);
      }
    }
    for (int broker : expected) {
      if (!Double.valueOf(1).equals(brokerUp.get(broker))) {
        network.remove(broker); urp.remove(broker); errors.add("KAFKA_EXPORTER_UNAVAILABLE:" + broker);
      }
    }
    if (!Double.valueOf(1).equals(cruiseUp)) { actions.clear(); errors.add("CRUISE_CONTROL_EXPORTER_UNAVAILABLE"); }
    if (!network.keySet().equals(expected)) errors.add("PROMETHEUS_NETWORK_INCOMPLETE");
    if (!urp.keySet().equals(expected)) errors.add("PROMETHEUS_URP_INCOMPLETE");
    if (!actions.keySet().equals(ACTIONS)) errors.add("PROMETHEUS_ACTIONS_INCOMPLETE");
    return new Reading(Map.copyOf(network), urp.keySet().equals(expected) ? sum(urp) : null,
        actions.keySet().equals(ACTIONS) ? sum(actions) : null, List.copyOf(errors));
  }

  private Sample latest(JsonNode samples) {
    if (!samples.isArray() || samples.isEmpty()) throw new IllegalArgumentException("samples required");
    double previous = Double.NEGATIVE_INFINITY;
    Sample last = null;
    for (JsonNode value : samples) {
      last = sample(value);
      if (last.time() <= previous) throw new IllegalArgumentException("unordered samples");
      previous = last.time();
    }
    double now = clock.instant().toEpochMilli() / 1000.0;
    if (last.time() < now - 15 || last.time() > now + 1) throw new IllegalArgumentException("stale or future sample");
    return last;
  }

  private static Sample sample(JsonNode node) {
    if (!node.isArray() || node.size() != 2 || !node.get(0).isNumber() || !node.get(1).isTextual()) {
      throw new IllegalArgumentException("invalid sample");
    }
    double time = node.get(0).doubleValue();
    double value = Double.parseDouble(node.get(1).textValue());
    if (!Double.isFinite(time) || time < 0 || !Double.isFinite(value) || value < 0) {
      throw new IllegalArgumentException("invalid sample number");
    }
    return new Sample(time, value);
  }
  private static double sum(Map<?, Double> values) { return values.values().stream().mapToDouble(Double::doubleValue).sum(); }
  private record Sample(double time, double value) { }
  record Reading(Map<Integer, Double> networkOutBytesPerSecond, Double underReplicatedPartitions,
      Double executorReplicaActions, List<String> errors) { }
}
