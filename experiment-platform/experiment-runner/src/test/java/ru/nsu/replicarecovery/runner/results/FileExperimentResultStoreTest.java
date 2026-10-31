package ru.nsu.replicarecovery.runner.results;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import ru.nsu.replicarecovery.model.ExperimentSpec;
import ru.nsu.replicarecovery.model.ExperimentState;
import ru.nsu.replicarecovery.runner.config.ExperimentSpecLoader;
import ru.nsu.replicarecovery.runner.domain.ExperimentOutcome;
import ru.nsu.replicarecovery.runner.lab.CleanupResult;
import ru.nsu.replicarecovery.runner.metrics.MonitoringSnapshot;

class FileExperimentResultStoreTest {
  @TempDir Path root;
  static final Instant START = Instant.parse("2026-10-28T10:00:00Z");
  final AtomicReference<Instant> now = new AtomicReference<>(START);
  final ObjectMapper json = new ObjectMapper().findAndRegisterModules();
  FileExperimentResultStore store;

  @BeforeEach
  void setUp() {
    Clock clock = mock(Clock.class);
    when(clock.instant()).thenAnswer(invocation -> now.get());
    store = new FileExperimentResultStore(root, clock);
  }

  @Test
  void writesFiveFilesNormalizedConfigAndFinalOutcomeAfterCleanup() throws Exception {
    store.create("smoke", spec());
    run("smoke");
    store.recordTimings("smoke", START, START);
    store.appendMetric("smoke", row(START, 120.0));
    now.set(START.plusSeconds(2));
    store.appendMetric("smoke", row(now.get(), 90.0));
    store.appendDecision("smoke", Map.of("reason", "SLO_P99_EXCEEDED"));
    store.appendEvent("smoke", "runner", "INFO", "message with\nnewline");
    now.set(START.plusSeconds(5));
    store.recordRecoveryFinished("smoke", now.get());
    store.transition("smoke", ExperimentState.STOPPING);
    store.transition("smoke", ExperimentState.CLEANING_UP);
    now.set(START.plusSeconds(15));
    var summary = store.finish("smoke", CleanupResult.ok());

    try (var files = Files.list(root.resolve("smoke"))) {
      assertThat(files.map(path -> path.getFileName().toString())).containsExactlyInAnyOrder(
          "experiment.json", "metrics.csv", "decisions.jsonl", "events.log", "summary.json");
    }
    assertThat(summary.finalState()).isEqualTo(ExperimentState.COMPLETED);
    assertThat(summary.filesComplete()).isTrue();
    assertThat(summary.sloViolationAreaMsSeconds()).isEqualTo(40);
    assertThat(summary.recoveryTimeSeconds()).isEqualTo(5);
    assertThat(summary.totalDurationSeconds()).isEqualTo(15);
    assertThat(store.get("smoke").terminal()).isTrue();
    assertThat(json.readTree(root.resolve("smoke/experiment.json").toFile())
        .path("spec").path("recovery").path("safeFallbackThrottleMiBps").asLong()).isEqualTo(20);
    assertThat(Files.readAllLines(root.resolve("smoke/decisions.jsonl"))).hasSize(1);
    for (String line : Files.readAllLines(root.resolve("smoke/events.log"))) {
      assertThat(json.readTree(line).path("timestamp").asText()).endsWith("Z");
    }
    assertThat(store.finish("smoke", CleanupResult.ok())).isEqualTo(summary);
    assertThatThrownBy(() -> store.appendMetric("smoke", row(now.get(), 1.0)))
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  void rejectsInvalidTransitionsUnattemptedCleanupAndOverwrite() {
    store.create("one", spec());
    assertThatThrownBy(() -> store.transition("one", ExperimentState.COMPLETED))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> store.transition("one", ExperimentState.CLEANING_UP))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> store.transition("one", ExperimentState.STOPPING))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> store.planOutcome("one", ExperimentOutcome.COMPLETED, "COMPLETED"))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> store.finish("one", CleanupResult.ok()))
        .isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(() -> store.create("one", spec())).isInstanceOf(RuntimeException.class);
    assertThatThrownBy(() -> store.create("../escape", spec())).isInstanceOf(IllegalArgumentException.class);
    store.planOutcome("one", ExperimentOutcome.CANCELLED, "USER_STOP");
    assertThat(store.get("one").state()).isEqualTo(ExperimentState.STOPPING);
    assertThat(store.get("one").plannedOutcome()).isEqualTo(ExperimentOutcome.CANCELLED);
    store.transition("one", ExperimentState.CLEANING_UP);
    assertThatThrownBy(() -> store.finish("one", new CleanupResult(false, false, List.of())))
        .isInstanceOf(IllegalArgumentException.class);
    assertThat(store.finish("one", CleanupResult.ok()).finalState()).isEqualTo(ExperimentState.CANCELLED);
  }

  @Test
  void failedCleanupCannotBecomeSuccessfulCompletion() {
    store.create("failed", spec());
    store.planOutcome("failed", ExperimentOutcome.FAILED, "RUNNER_RESTART");
    var summary = store.finish("failed", new CleanupResult(true, false, List.of("adapter unavailable")));
    assertThat(summary.finalState()).isEqualTo(ExperimentState.FAILED);
    assertThat(summary.reason()).isEqualTo("RUNNER_RESTART");
    assertThat(summary.cleanup().success()).isFalse();
  }

  @Test
  void cleanupFailureExplainsWhyANormalRunDidNotComplete() {
    store.create("cleanup-failed", spec());
    run("cleanup-failed");
    store.transition("cleanup-failed", ExperimentState.STOPPING);
    store.transition("cleanup-failed", ExperimentState.CLEANING_UP);
    var summary = store.finish("cleanup-failed", new CleanupResult(true, false, List.of("test failure")));
    assertThat(summary.finalState()).isEqualTo(ExperimentState.FAILED);
    assertThat(summary.reason()).isEqualTo("CLEANUP_FAILED");
    assertThat(store.get("cleanup-failed").reason()).isEqualTo("CLEANUP_FAILED");
  }

  @Test
  void concurrentAppendPreservesAllLinesAndJsonEscaping() throws Exception {
    store.create("parallel", spec());
    var workers = Executors.newFixedThreadPool(8);
    try {
      for (int index = 0; index < 200; index++) {
        int sequence = index;
        workers.submit(() -> {
          store.appendDecision("parallel", Map.of("sequence", sequence, "reason", "line\nquote\""));
          store.appendEvent("parallel", "test", "INFO", "event " + sequence);
          store.appendMetric("parallel", row(START.plusMillis(sequence), 10.0));
        });
      }
    } finally {
      workers.shutdown();
    }
    assertThat(workers.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
    List<String> lines = Files.readAllLines(root.resolve("parallel/decisions.jsonl"));
    assertThat(lines).hasSize(200);
    var sequences = new java.util.HashSet<Integer>();
    for (String line : lines) {
      var decision = json.readTree(line);
      sequences.add(decision.path("sequence").asInt());
      assertThat(decision.path("timestamp").asText()).endsWith("Z");
    }
    assertThat(sequences).hasSize(200);
    assertThat(Files.readAllLines(root.resolve("parallel/events.log"))).hasSize(201);
    var metrics = Files.readAllLines(root.resolve("parallel/metrics.csv"));
    assertThat(metrics).hasSize(201);
    assertThat(metrics).allSatisfy(line -> assertThat(line.split(",", -1)).hasSize(14));
  }

  @Test
  void persistedVersionCannotBeCoercedFromFloatOrString() throws Exception {
    store.create("schema", spec());
    Path snapshot = root.resolve("schema/experiment.json");
    var original = json.readTree(snapshot.toFile());
    for (var version : List.of(json.getNodeFactory().numberNode(1.5), json.getNodeFactory().textNode("1"))) {
      var changed = original.deepCopy();
      ((com.fasterxml.jackson.databind.node.ObjectNode) changed).set("schemaVersion", version);
      Files.writeString(snapshot, changed.toString());
      assertThatThrownBy(() -> store.get("schema")).isInstanceOf(RuntimeException.class);
    }
  }

  @Test
  void persistsPerBrokerMonitoringWithQuotedJsonAndPreservesUnknownSignals() throws Exception {
    store.create("monitoring", spec());
    var monitoring = new MonitoringSnapshot(START, false, Map.of(1, 12.0), Map.of(1, 1048576.0),
        Map.of(1, 128.0), Map.of(), null, 0.0, "NO_TASK_IN_PROGRESS",
        List.of("missing disk, source \"docker\"", "remote\nerror"));
    var sample = new ExperimentMetricRow(START, 90.0, 0, 100, 99, 20, 100, 1, 3,
        false, 12.0, 128.0, null, monitoring);
    store.appendMetric("monitoring", sample);
    assertThat(store.metrics("monitoring")).containsExactly(sample);
    assertThat(Files.readAllLines(root.resolve("monitoring/metrics.csv"))).hasSize(2);
    assertThat(store.metrics("monitoring").get(0).monitoring().underReplicatedPartitions()).isNull();
  }

  @Test
  void readsOldThirteenColumnMetricsWithoutInventingMonitoring() throws Exception {
    store.create("legacy", spec());
    String header = FileExperimentResultStore.HEADER.replace(",monitoringSnapshot", "");
    Files.writeString(root.resolve("legacy/metrics.csv"), header
        + START + ",90.0,0.0,100.0,99.0,20,100,1,3,false,10.0,128.0,256.0\n");
    var sample = store.metrics("legacy").get(0);
    assertThat(sample.produceP99Ms()).isEqualTo(90.0);
    assertThat(sample.monitoring()).isNull();
    assertThat(sample.brokerCpuPercent()).isEqualTo(10.0);
  }

  @Test
  void failedAtomicSummaryDoesNotWriteATerminalMarker() throws Exception {
    store.create("atomic", spec());
    store.planOutcome("atomic", ExperimentOutcome.FAILED, "TEST_FAILURE");
    Files.createDirectory(root.resolve("atomic/summary.json"));
    assertThatThrownBy(() -> store.finish("atomic", CleanupResult.ok()))
        .isInstanceOf(RuntimeException.class);
    assertThat(store.get("atomic").state()).isEqualTo(ExperimentState.CLEANING_UP);
    assertThat(Files.isDirectory(root.resolve("atomic/summary.json"))).isTrue();
    assertThat(root.resolve("atomic/summary.json.tmp")).doesNotExist();
  }

  static ExperimentSpec spec() {
    return new ExperimentSpecLoader().load("""
        schemaVersion: 1
        name: runner-smoke
        timeout: 30m
        slo: {produceP99Ms: 100, maxErrorRate: 0.01}
        load:
          mode: CONSTANT
          messageSizeBytes: 1024
          schedule: [{duration: 30s, requestsPerSecond: 100}]
        recovery: {throttlePolicy: AIMD, initialThrottleMiBps: 20, minThrottleMiBps: 1, maxThrottleMiBps: 100}
        resources: {profile: normal}
        fault: {action: STOP_BROKER, target: broker-2, at: 1s}
        """);
  }

  static ExperimentMetricRow row(Instant timestamp, Double latency) {
    return new ExperimentMetricRow(timestamp, latency, 0, 100, 99,
        20, 100, 1, 3, false, null, null, null);
  }

  void run(String id) {
    for (ExperimentState state : new ExperimentState[] {ExperimentState.VALIDATING,
        ExperimentState.PREPARING, ExperimentState.WARMING_UP, ExperimentState.RUNNING}) {
      store.transition(id, state);
    }
  }
}
