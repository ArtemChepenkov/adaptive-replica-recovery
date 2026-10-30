package ru.nsu.replicarecovery.runner.client;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.WireMockServer;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import ru.nsu.replicarecovery.model.ExperimentSpec;
import ru.nsu.replicarecovery.model.ControlSettingsSnapshot;
import ru.nsu.replicarecovery.model.LoadMetricsSnapshot;
import ru.nsu.replicarecovery.runner.config.ExperimentSpecLoader;

class HttpClientsTest {
  WireMockServer server;
  List<Duration> pauses;
  HttpJsonTransport transport;

  @BeforeEach
  void startServer() {
    server = new WireMockServer(com.github.tomakehurst.wiremock.core.WireMockConfiguration.options().dynamicPort());
    server.start();
    server.stubFor(get(urlPathEqualTo("/kafkacruisecontrol/user_tasks")).atPriority(10)
        .willReturn(okJson("{\"userTasks\":[]}")));
    pauses = new ArrayList<>();
    transport = new HttpJsonTransport(server.baseUrl(), Duration.ofSeconds(2), Duration.ofSeconds(3), pauses::add);
  }

  @AfterEach
  void stopServer() { server.stop(); }

  @Test
  void sendsLoadSettingsAndDoesNotRetryStart() throws Exception {
    server.stubFor(post(urlEqualTo("/api/load/start")).willReturn(aResponse().withStatus(202)
        .withBody("{\"state\":\"RUNNING\",\"runId\":\"load-1\",\"cleanupComplete\":false,\"outstandingRequests\":0,\"error\":null}")));
    var client = new HttpLoadGeneratorClient(transport);
    assertThat(client.start(spec()).runId()).isEqualTo("load-1");
    var sent = new ObjectMapper().readTree(server.getAllServeEvents().get(0).getRequest().getBodyAsString());
    assertThat(sent.path("load").path("messageSizeBytes").asInt()).isEqualTo(1024);
    assertThat(sent.path("slo").path("produceP99Ms").asDouble()).isEqualTo(100);
    assertThat(sent.path("recovery").path("initialThrottleMiBps").asInt()).isEqualTo(20);
    assertThat(sent.path("seed").asLong()).isEqualTo(20260910);
    server.resetAll();
    server.stubFor(post(urlEqualTo("/api/load/start")).willReturn(aResponse().withStatus(503).withBody("{}")));
    assertThatThrownBy(() -> client.start(spec())).isInstanceOf(RemoteCallException.class);
    server.verify(1, postRequestedFor(urlEqualTo("/api/load/start")));
    assertThat(pauses).isEmpty();
  }

  @Test
  void retriesTransientGetsExactlyThreeTimesAndHonorsReadTimeout() {
    server.stubFor(get(urlEqualTo("/api/load/status")).willReturn(aResponse().withStatus(503).withBody("{}")));
    var client = new HttpLoadGeneratorClient(transport);
    assertThatThrownBy(client::status).isInstanceOf(RemoteCallException.class);
    server.verify(3, getRequestedFor(urlEqualTo("/api/load/status")));
    assertThat(pauses).containsExactly(Duration.ofSeconds(1), Duration.ofSeconds(2));
    server.resetAll();
    pauses.clear();
    server.stubFor(get(urlEqualTo("/api/load/status")).willReturn(aResponse().withFixedDelay(1000).withBody("{}")));
    var shortRead = new HttpLoadGeneratorClient(new HttpJsonTransport(server.baseUrl(),
        Duration.ofSeconds(2), Duration.ofMillis(150), pauses::add));
    assertThatThrownBy(shortRead::status).isInstanceOf(RemoteCallException.class);
    server.verify(3, getRequestedFor(urlEqualTo("/api/load/status")));
  }

  @Test
  void decodesAnActualLoadMetricsContractWithItsControlSettings() throws Exception {
    Instant end = Instant.parse("2026-10-29T10:00:00Z");
    var snapshot = new LoadMetricsSnapshot(end.minusSeconds(60), end, end, 10,
        1.0, 2.0, 3.0, 1, 0.1, 100, 90, 9216, 2,
        new ControlSettingsSnapshot(spec().slo(), spec().recovery()));
    var mapper = new ObjectMapper().findAndRegisterModules()
        .disable(com.fasterxml.jackson.databind.SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
        .disable(com.fasterxml.jackson.databind.SerializationFeature.WRITE_DURATIONS_AS_TIMESTAMPS);
    server.stubFor(get(urlEqualTo("/api/load/metrics/current")).willReturn(okJson(mapper.writeValueAsString(snapshot))));
    assertThat(new HttpLoadGeneratorClient(transport).metrics()).isEqualTo(snapshot);
  }

  @Test
  void readTimeoutIncludesASlowResponseBodyAfterHeadersArrive() {
    server.stubFor(get(urlEqualTo("/api/load/status")).willReturn(okJson(
        "{\"state\":\"RUNNING\",\"runId\":\"slow-body\",\"cleanupComplete\":false,\"outstandingRequests\":0}")
        .withChunkedDribbleDelay(6, 3000)));
    var bounded = new HttpLoadGeneratorClient(new HttpJsonTransport(server.baseUrl(),
        Duration.ofSeconds(2), Duration.ofMillis(500), pauses::add));
    assertThatThrownBy(bounded::status).isInstanceOf(RemoteCallException.class);
    server.verify(3, getRequestedFor(urlEqualTo("/api/load/status")));
  }

  @Test
  void malformedMetricsNeverBecomeHealthyZeroes() {
    server.stubFor(get(urlEqualTo("/api/load/metrics/current")).willReturn(okJson("{}")));
    assertThatThrownBy(() -> new HttpLoadGeneratorClient(transport).metrics()).isInstanceOf(RemoteCallException.class);
    server.verify(1, getRequestedFor(urlEqualTo("/api/load/metrics/current")));
  }

  @Test
  void mapsThrottleAndFlagsAndSelectsOnlyItsUserTask() {
    stubStart(true);
    stubTaskAndExecutor("Completed", "NO_TASK_IN_PROGRESS");
    server.stubFor(get(urlPathEqualTo("/kafkacruisecontrol/recovery_control")).willReturn(okJson(controlJson())));
    var client = new HttpCruiseControlClient(transport);
    assertThat(client.startRecovery(spec())).isEqualTo("task-1");
    var status = client.status();
    assertThat(status.completed()).isTrue();
    assertThat(status.failed()).isFalse();
    assertThat(status.active()).isFalse();
    assertThat(status.remainingBytes()).isZero();
    assertThat(status.measurementAvailable()).isTrue();
    server.verify(postRequestedFor(urlPathEqualTo("/kafkacruisecontrol/rebalance"))
        .withQueryParam("replication_throttle", equalTo("20971520"))
        .withQueryParam("recovery_control", equalTo("true"))
        .withQueryParam("replica_movement_strategies", equalTo("RiskAwareReplicaMovementStrategy")));
  }

  @Test
  void completedTaskDoesNotMeanIdleExecutor() {
    stubStart(true);
    stubTaskAndExecutor("Completed", "INTER_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS");
    server.stubFor(get(urlPathEqualTo("/kafkacruisecontrol/recovery_control")).willReturn(okJson(controlJson())));
    var client = new HttpCruiseControlClient(transport);
    client.startRecovery(spec());
    assertThat(client.status().completed()).isFalse();
    assertThat(client.status().active()).isTrue();
  }

  @Test
  void missingExtensionPreservesUnknownTelemetryAndDisabledRiskOmitsStrategy() {
    stubStart(true);
    stubTaskAndExecutor("Completed", "NO_TASK_IN_PROGRESS");
    server.stubFor(get(urlPathEqualTo("/kafkacruisecontrol/recovery_control")).willReturn(aResponse().withStatus(404)));
    var client = new HttpCruiseControlClient(transport);
    client.startRecovery(new ExperimentSpecLoader().load(yaml().replace("throttlePolicy: AIMD",
        "throttlePolicy: FIXED, adaptiveThrottleEnabled: false, riskOrderingEnabled: false")));
    var status = client.status();
    assertThat(status.completed()).isTrue();
    assertThat(status.remainingBytes()).isNull();
    assertThat(status.safeState()).isNull();
    assertThat(status.measurementAvailable()).isFalse();
    server.verify(postRequestedFor(urlPathEqualTo("/kafkacruisecontrol/rebalance"))
        .withQueryParam("recovery_control", equalTo("false"))
        .withoutQueryParam("replica_movement_strategies"));
  }

  @Test
  void ambiguousStartCannotClaimIdleAndIsNotRetried() {
    stubStart(false);
    var client = new HttpCruiseControlClient(transport);
    assertThatThrownBy(() -> client.startRecovery(spec())).isInstanceOf(RemoteCallException.class);
    assertThatThrownBy(client::status).isInstanceOf(RemoteCallException.class).hasMessageContaining("ambiguous");
    server.verify(1, postRequestedFor(urlPathEqualTo("/kafkacruisecontrol/rebalance")));
  }

  @Test
  void completedWithErrorStillNeedsAnIdleExecutorBeforeCleanupIsVerified() {
    stubStart(true);
    stubTaskAndExecutor("CompletedWithError", "INTER_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS");
    server.stubFor(get(urlPathEqualTo("/kafkacruisecontrol/recovery_control")).willReturn(okJson(controlJson())));
    var client = new HttpCruiseControlClient(transport);
    client.startRecovery(spec());
    var status = client.status();
    assertThat(status.failed()).isTrue();
    assertThat(status.completed()).isFalse();
    assertThat(status.active()).isTrue();
  }

  @ParameterizedTest
  @ValueSource(ints = {400, 404, 409, 429})
  void explicitStartRejectionDoesNotPoisonStatusOrReset(int responseStatus) {
    server.stubFor(post(urlPathEqualTo("/kafkacruisecontrol/rebalance"))
        .willReturn(aResponse().withStatus(responseStatus).withBody("{}")));
    server.stubFor(get(urlPathEqualTo("/kafkacruisecontrol/state"))
        .willReturn(okJson("{\"ExecutorState\":{\"state\":\"NO_TASK_IN_PROGRESS\"}}")));
    var client = new HttpCruiseControlClient(transport);
    assertThatThrownBy(() -> client.startRecovery(spec())).isInstanceOf(RemoteCallException.class);
    var status = client.status();
    assertThat(status.active()).isFalse();
    assertThat(status.completed()).isFalse();
    assertThat(status.reason()).isEqualTo("NO_TRACKED_EXECUTION");
    server.verify(1, postRequestedFor(urlPathEqualTo("/kafkacruisecontrol/rebalance")));
  }

  @ParameterizedTest
  @ValueSource(ints = {408, 500, 503})
  void timeoutAndServerFailureStillRequireExecutionOwnershipVerification(int responseStatus) {
    server.stubFor(post(urlPathEqualTo("/kafkacruisecontrol/rebalance"))
        .willReturn(aResponse().withStatus(responseStatus).withBody("{}")));
    var client = new HttpCruiseControlClient(transport);
    assertThatThrownBy(() -> client.startRecovery(spec())).isInstanceOf(RemoteCallException.class);
    assertThatThrownBy(client::status).isInstanceOf(RemoteCallException.class).hasMessageContaining("ambiguous");
    server.verify(1, postRequestedFor(urlPathEqualTo("/kafkacruisecontrol/rebalance")));
  }

  @Test
  void activeDiagnosticGetDoesNotBlockAnOtherwiseIdleUntrackedClient() {
    server.stubFor(get(urlPathEqualTo("/kafkacruisecontrol/state"))
        .willReturn(okJson("{\"ExecutorState\":{\"state\":\"NO_TASK_IN_PROGRESS\"}}")));
    server.stubFor(get(urlPathEqualTo("/kafkacruisecontrol/user_tasks")).willReturn(okJson(
        "{\"userTasks\":[{\"Status\":\"Active\",\"RequestURL\":\"GET /kafkacruisecontrol/state?json=true\"},"
            + "{\"Status\":\"Completed\",\"RequestURL\":\"POST /kafkacruisecontrol/rebalance\"}]}")));
    assertThat(new HttpCruiseControlClient(transport).status().active()).isFalse();
  }

  @ParameterizedTest
  @ValueSource(strings = {"POST", "PUT", "PATCH", "DELETE"})
  void activeUntrackedMutationBlocksIdleEvenBeforeExecutorStarts(String method) {
    server.stubFor(get(urlPathEqualTo("/kafkacruisecontrol/state"))
        .willReturn(okJson("{\"ExecutorState\":{\"state\":\"NO_TASK_IN_PROGRESS\"}}")));
    server.stubFor(get(urlPathEqualTo("/kafkacruisecontrol/user_tasks")).willReturn(okJson(
        "{\"userTasks\":[{\"Status\":\"Active\",\"RequestURL\":\"" + method
            + " /kafkacruisecontrol/rebalance?json=true\"}]}")));
    var status = new HttpCruiseControlClient(transport).status();
    assertThat(status.active()).isTrue();
    assertThat(status.reason()).isEqualTo("UNTRACKED_EXECUTION");
    assertThat(status.completed()).isFalse();
  }

  @Test
  void malformedUntrackedTaskMetadataCannotBeReportedIdle() {
    server.stubFor(get(urlPathEqualTo("/kafkacruisecontrol/state"))
        .willReturn(okJson("{\"ExecutorState\":{\"state\":\"NO_TASK_IN_PROGRESS\"}}")));
    for (String body : new String[] {"{\"userTasks\":{}}",
        "{\"userTasks\":[{\"Status\":\"Active\"}]}",
        "{\"userTasks\":[{\"Status\":\"Unknown\",\"RequestURL\":\"GET /state\"}]}"}) {
      server.stubFor(get(urlPathEqualTo("/kafkacruisecontrol/user_tasks")).willReturn(okJson(body)));
      assertThatThrownBy(() -> new HttpCruiseControlClient(transport).status())
          .isInstanceOf(RemoteCallException.class);
    }
  }

  @Test
  void stopDoesNotForgetTheTaskBeforeItIsTerminal() {
    stubStart(true);
    stubTaskAndExecutor("InExecution", "INTER_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS");
    server.stubFor(get(urlPathEqualTo("/kafkacruisecontrol/recovery_control")).willReturn(okJson(controlJson())));
    server.stubFor(post(urlPathEqualTo("/kafkacruisecontrol/stop_proposal_execution")).willReturn(okJson("{}")));
    var client = new HttpCruiseControlClient(transport);
    client.startRecovery(spec());
    client.stopRecovery();
    assertThat(client.status().active()).isTrue();
    server.verify(1, postRequestedFor(urlPathEqualTo("/kafkacruisecontrol/stop_proposal_execution")));
  }

  @Test
  void emptyPrometheusVectorIsMissingAndNotZero() {
    server.stubFor(get(urlPathEqualTo("/api/v1/query")).willReturn(okJson(
        "{\"status\":\"success\",\"data\":{\"resultType\":\"vector\",\"result\":[]}}")));
    var metrics = new HttpPrometheusClient(transport, "cpu", "network", "disk").current();
    assertThat(metrics.brokerCpuPercent()).isNull();
    assertThat(metrics.brokerNetworkOutBytesPerSecond()).isNull();
    assertThat(metrics.brokerDiskWriteBytesPerSecond()).isNull();
  }

  @Test
  void parsesFiniteAggregatedPrometheusValues() {
    server.stubFor(get(urlPathEqualTo("/api/v1/query")).willReturn(okJson(
        "{\"status\":\"success\",\"data\":{\"resultType\":\"vector\",\"result\":[{\"value\":[1,\"12.5\"]}]}}")));
    assertThat(new HttpPrometheusClient(transport, "cpu", "network", "disk").current())
        .isEqualTo(new PrometheusClient.ResourceMetrics(12.5, 12.5, 12.5));
  }

  @Test
  void rejectsNonFinitePrometheusValues() {
    server.stubFor(get(urlPathEqualTo("/api/v1/query")).willReturn(okJson(
        "{\"status\":\"success\",\"data\":{\"resultType\":\"vector\",\"result\":[{\"value\":[1,\"NaN\"]}]}}")));
    assertThatThrownBy(() -> new HttpPrometheusClient(transport, "cpu", "network", "disk").current())
        .isInstanceOf(RemoteCallException.class);
  }

  void stubStart(boolean header) {
    var response = aResponse().withStatus(200).withBody("{\"progress\":[]}");
    if (header) { response.withHeader("User-Task-ID", "task-1"); }
    server.stubFor(post(urlPathEqualTo("/kafkacruisecontrol/rebalance")).willReturn(response));
  }

  void stubTaskAndExecutor(String taskState, String executorState) {
    server.stubFor(get(urlPathEqualTo("/kafkacruisecontrol/user_tasks")).willReturn(okJson(
        "{\"userTasks\":[{\"UserTaskId\":\"other-task\",\"Status\":\"CompletedWithError\"},"
            + "{\"UserTaskId\":\"task-1\",\"Status\":\"" + taskState + "\"}]}")));
    server.stubFor(get(urlPathEqualTo("/kafkacruisecontrol/state")).willReturn(okJson(
        "{\"ExecutorState\":{\"state\":\"" + executorState + "\"}}")));
  }

  static String controlJson() {
    return "{\"currentThrottleBytesPerSecond\":20971520,\"remainingBytes\":0,\"progressBytesPerSecond\":100,"
        + "\"criticalPartitions\":0,\"safeState\":true,\"reason\":\"SLO_HEALTHY\",\"lastAction\":\"HOLD\"}";
  }

  static ExperimentSpec spec() { return new ExperimentSpecLoader().load(yaml()); }
  static String yaml() {
    return """
        schemaVersion: 1
        name: client-smoke
        timeout: 30m
        slo: {produceP99Ms: 100, maxErrorRate: 0.01}
        load: {mode: CONSTANT, messageSizeBytes: 1024, schedule: [{duration: 30s, requestsPerSecond: 100}]}
        recovery: {throttlePolicy: AIMD, initialThrottleMiBps: 20, minThrottleMiBps: 1, maxThrottleMiBps: 100}
        resources: {profile: normal}
        fault: {action: STOP_BROKER, target: broker-2, at: 1s}
        """;
  }
}
