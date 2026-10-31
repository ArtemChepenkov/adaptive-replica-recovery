package ru.nsu.replicarecovery.runner.metrics;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.assertj.core.api.Assertions.*;

import com.github.tomakehurst.wiremock.WireMockServer;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MonitoringCollectorTest {
  private static final Instant NOW = Instant.ofEpochSecond(1800000010);
  private static final Clock CLOCK = Clock.fixed(NOW, ZoneOffset.UTC);
  private static final Set<Integer> BROKERS = Set.of(1, 2);
  private WireMockServer server;
  @BeforeEach void startServer() {
    server = new WireMockServer(com.github.tomakehurst.wiremock.core.WireMockConfiguration.options().dynamicPort());
    server.start();
  }
  @AfterEach void stopServer() { server.stop(); }

  @Test void parsesMatrixPerBrokerAndCombinesRealSourcesWithoutReplacingSloMetrics() {
    prometheus(matrix());
    var readings = new PrometheusMonitoringSource(server.baseUrl(), BROKERS, CLOCK).get();
    assertThat(readings.networkOutBytesPerSecond()).containsEntry(1, 100.0).containsEntry(2, 200.0);
    assertThat(readings.underReplicatedPartitions()).isEqualTo(3.0);
    assertThat(readings.executorReplicaActions()).isEqualTo(2.0);
    try (var collector = collector(() -> readings, () -> "NO_TASK_IN_PROGRESS")) {
      var resources = collector.current();
      assertThat(resources.monitoring().available()).isTrue();
      assertThat(resources.brokerCpuPercent()).isEqualTo(15.0);
      assertThat(resources.brokerNetworkOutBytesPerSecond()).isEqualTo(300.0);
      assertThat(resources.monitoring().brokerMemoryBytes()).containsEntry(1, 1048576.0);
      assertThat(resources.monitoring().executionState()).isEqualTo("NO_TASK_IN_PROGRESS");
    }
    server.verify(1, getRequestedFor(urlPathEqualTo("/api/v1/query")));
  }

  @Test void emptyMatrixIsUnavailableAndKeepsKnownDockerValues() {
    prometheus("{\"status\":\"success\",\"data\":{\"resultType\":\"matrix\",\"result\":[]}}");
    var source = new PrometheusMonitoringSource(server.baseUrl(), BROKERS, CLOCK);
    try (var collector = collector(source, () -> "NO_TASK_IN_PROGRESS")) {
      var resources = collector.current();
      assertThat(resources.monitoring().available()).isFalse();
      assertThat(resources.monitoring().brokerCpuPercent()).hasSize(2);
      assertThat(resources.brokerNetworkOutBytesPerSecond()).isNull();
      assertThat(resources.monitoring().underReplicatedPartitions()).isNull();
      assertThat(resources.monitoring().executorReplicaActions()).isNull();
    }
  }

  @Test void staleSamplesCounterResetAndNonFiniteValuesNeverBecomeZeroes() {
    for (String broken : List.of(matrix().replace("1800000010", "1799999900"),
        matrix().replace("1500", "500"), matrix().replace("1500", "NaN"))) {
      prometheus(broken);
      var data = new PrometheusMonitoringSource(server.baseUrl(), BROKERS, CLOCK).get();
      assertThat(data.networkOutBytesPerSecond()).doesNotContainKey(1);
      assertThat(data.errors()).isNotEmpty();
    }
  }

  @Test void duplicateBrokerSeriesAreRejected() {
    prometheus(matrix().replace("\"broker_id\":\"2\"", "\"broker_id\":\"1\""));
    var data = new PrometheusMonitoringSource(server.baseUrl(), BROKERS, CLOCK).get();
    assertThat(data.networkOutBytesPerSecond()).doesNotContainKey(1);
    assertThat(data.underReplicatedPartitions()).isNull();
    assertThat(data.errors()).isNotEmpty();
  }

  @Test void unavailablePrometheusOrCruiseDoesNotThrowAndIsCached() {
    AtomicInteger calls = new AtomicInteger();
    try (var collector = new MonitoringCollector(BROKERS, CLOCK, Duration.ofSeconds(5),
        () -> { calls.incrementAndGet(); throw new IllegalStateException("offline"); },
        MonitoringCollectorTest::docker, () -> { throw new IllegalStateException("offline"); })) {
      assertThat(collector.current().monitoring().available()).isFalse();
      assertThat(collector.current().monitoring().available()).isFalse();
      assertThat(calls).hasValue(1);
      assertThat(collector.snapshot().errors()).contains("PROMETHEUS_UNAVAILABLE", "CRUISE_CONTROL_UNAVAILABLE");
    }
  }

  @Test void executorStateUsesItsOwnReadOnlyStateEndpoint() {
    server.stubFor(get(urlPathEqualTo("/kafkacruisecontrol/state"))
        .willReturn(okJson("{\"ExecutorState\":{\"state\":\"INTER_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS\"}}")));
    assertThat(new CruiseControlMonitoringSource(server.baseUrl()).get())
        .isEqualTo("INTER_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS");
    server.verify(0, postRequestedFor(anyUrl()));
  }

  @Test void failedExporterDoesNotBecomeHealthyThroughFreshRecordingRuleSamples() {
    prometheus(matrix().replace("\"broker_id\":\"1\"},\"values\":[[1800000010,\"1\"]]}",
        "\"broker_id\":\"1\"},\"values\":[[1800000010,\"0\"]]}"));
    var data = new PrometheusMonitoringSource(server.baseUrl(), BROKERS, CLOCK).get();
    assertThat(data.networkOutBytesPerSecond()).doesNotContainKey(1);
    assertThat(data.underReplicatedPartitions()).isNull();
    assertThat(data.errors()).contains("KAFKA_EXPORTER_UNAVAILABLE:1");
  }

  @Test void diagnosticHttpFailureIsNotRetriedOnTheSloWorker() {
    server.stubFor(get(urlPathEqualTo("/api/v1/query")).willReturn(aResponse().withStatus(503)));
    try (var collector = collector(new PrometheusMonitoringSource(server.baseUrl(), BROKERS, CLOCK), () -> "NO_TASK_IN_PROGRESS")) {
      assertThat(collector.current().monitoring().available()).isFalse();
    }
    server.verify(1, getRequestedFor(urlPathEqualTo("/api/v1/query")));
  }

  private MonitoringCollector collector(java.util.function.Supplier<PrometheusMonitoringSource.Reading> prometheus,
      java.util.function.Supplier<String> executor) {
    return new MonitoringCollector(BROKERS, CLOCK, Duration.ZERO, prometheus, MonitoringCollectorTest::docker, executor);
  }
  private static DockerStatsSource.Reading docker() {
    return new DockerStatsSource.Reading(Map.of(1, 10.0, 2, 20.0), Map.of(1, 1048576.0, 2, 2097152.0),
        Map.of(1, 100.0, 2, 200.0), List.of());
  }
  private void prometheus(String body) {
    server.resetAll();
    server.stubFor(get(urlPathEqualTo("/api/v1/query")).willReturn(okJson(body)));
  }
  private static String matrix() {
    return """
        {"status":"success","data":{"resultType":"matrix","result":[
        {"metric":{"__name__":"up","job":"kafka","broker_id":"1"},"values":[[1800000010,"1"]]},
        {"metric":{"__name__":"up","job":"kafka","broker_id":"2"},"values":[[1800000010,"1"]]},
        {"metric":{"__name__":"up","job":"cruise-control"},"values":[[1800000010,"1"]]},
        {"metric":{"__name__":"broker_network_out_bytes_total","broker_id":"1"},"values":[[1800000005,"1000"],[1800000010,"1500"]]},
        {"metric":{"__name__":"broker_network_out_bytes_total","broker_id":"2"},"values":[[1800000005,"2000"],[1800000010,"3000"]]},
        {"metric":{"__name__":"broker_under_replicated_partitions","broker_id":"1"},"values":[[1800000010,"1"]]},
        {"metric":{"__name__":"broker_under_replicated_partitions","broker_id":"2"},"values":[[1800000010,"2"]]},
        {"metric":{"__name__":"cruise_control_executor_replica_actions","type":"replica-action","state":"in-progress"},"values":[[1800000010,"1"]]},
        {"metric":{"__name__":"cruise_control_executor_replica_actions","type":"replica-action","state":"aborting"},"values":[[1800000010,"0"]]},
        {"metric":{"__name__":"cruise_control_executor_replica_actions","type":"intra-broker-replica-action","state":"in-progress"},"values":[[1800000010,"1"]]},
        {"metric":{"__name__":"cruise_control_executor_replica_actions","type":"intra-broker-replica-action","state":"aborting"},"values":[[1800000010,"0"]]}
        ]}}
        """;
  }
}
