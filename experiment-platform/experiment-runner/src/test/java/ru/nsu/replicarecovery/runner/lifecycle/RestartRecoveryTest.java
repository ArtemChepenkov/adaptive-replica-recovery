package ru.nsu.replicarecovery.runner.lifecycle;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import ru.nsu.replicarecovery.model.ExperimentState;
import ru.nsu.replicarecovery.runner.config.ExperimentSpecLoader;
import ru.nsu.replicarecovery.runner.domain.ExperimentOutcome;
import ru.nsu.replicarecovery.runner.lab.CleanupResult;
import ru.nsu.replicarecovery.runner.lab.LabController;
import ru.nsu.replicarecovery.runner.results.FileExperimentResultStore;

class RestartRecoveryTest {
  @TempDir Path root;
  final Clock clock = Clock.fixed(Instant.parse("2026-10-28T10:00:00Z"), ZoneOffset.UTC);

  @Test
  void restartsIncompleteRunsAfterCleanupAndLeavesTerminalRunsUntouched() throws Exception {
    var first = store();
    first.create("interrupted", spec());
    first.transition("interrupted", ExperimentState.VALIDATING);
    first.create("terminal", spec());
    first.planOutcome("terminal", ExperimentOutcome.CANCELLED, "USER_STOP");
    first.transition("terminal", ExperimentState.CLEANING_UP);
    first.finish("terminal", CleanupResult.ok());
    String previousSummary = Files.readString(root.resolve("terminal/summary.json"));
    AtomicInteger cleanups = new AtomicInteger();
    LabController lab = () -> {
      cleanups.incrementAndGet();
      assertThat(store().get("interrupted").state()).isEqualTo(ExperimentState.CLEANING_UP);
      return CleanupResult.ok();
    };
    new RestartRecovery(store(), lab).recover();

    assertThat(cleanups.get()).isEqualTo(1);
    assertThat(store().get("interrupted").state()).isEqualTo(ExperimentState.FAILED);
    var summary = new ObjectMapper().readTree(root.resolve("interrupted/summary.json").toFile());
    assertThat(summary.path("reason").asText()).isEqualTo("RUNNER_RESTART");
    assertThat(summary.path("cleanup").path("success").asBoolean()).isTrue();
    assertThat(Files.readString(root.resolve("interrupted/events.log"))).contains("RUNNER_RESTART_DETECTED");
    new RestartRecovery(store(), lab).recover();
    assertThat(cleanups.get()).isEqualTo(1);
    assertThat(Files.readString(root.resolve("terminal/summary.json"))).isEqualTo(previousSummary);
  }

  @Test
  void recordsCleanupFailureAndRefusesToDeclareTheApplicationReady() {
    store().create("interrupted", spec());
    assertThatThrownBy(() -> new RestartRecovery(store(), () -> {
      throw new IllegalStateException("adapter unavailable");
    }).recover()).isInstanceOf(IllegalStateException.class).hasMessageContaining("cleanup");
    assertThat(store().get("interrupted").state()).isEqualTo(ExperimentState.FAILED);
  }

  @Test
  void missingEventLogDoesNotPreventActualCleanup() throws Exception {
    store().create("missing-events", spec());
    Files.delete(root.resolve("missing-events/events.log"));
    LabController lab = mock(LabController.class);
    when(lab.cleanup()).thenReturn(CleanupResult.ok());
    assertThatThrownBy(() -> new RestartRecovery(store(), lab).recover())
        .isInstanceOf(IllegalStateException.class).hasMessageContaining("persistence errors=1");
    verify(lab).cleanup();
  }

  @Test
  void tornMetricsArePreservedAndDoNotInventObservationsDuringDowntime() throws Exception {
    var store = store();
    store.create("torn", spec());
    store.appendMetric("torn", new ru.nsu.replicarecovery.runner.results.ExperimentMetricRow(
        clock.instant(), 120.0, 0, 100, 99, 20, 100, 1, 3, false, null, null, null));
    Files.writeString(root.resolve("torn/metrics.csv"), "truncated", java.nio.file.StandardOpenOption.APPEND);
    Clock restarted = Clock.offset(clock, java.time.Duration.ofHours(2));
    var after = new FileExperimentResultStore(root, restarted);
    new RestartRecovery(after, CleanupResult::ok).recover();
    assertThat(after.get("torn").state()).isEqualTo(ExperimentState.FAILED);
    var summary = new ObjectMapper().readTree(root.resolve("torn/summary.json").toFile());
    assertThat(summary.path("filesComplete").asBoolean()).isFalse();
    assertThat(summary.path("observedDurationSeconds").asDouble()).isZero();
    assertThat(Files.readString(root.resolve("torn/metrics.csv"))).endsWith("truncated");
  }

  @Test
  void corruptMetadataStillTriggersCleanupBeforeFailingClosed() throws Exception {
    store().create("broken", spec());
    Files.writeString(root.resolve("broken/experiment.json"), "{broken");
    LabController lab = mock(LabController.class);
    when(lab.cleanup()).thenReturn(CleanupResult.ok());
    assertThatThrownBy(() -> new RestartRecovery(store(), lab).recover())
        .isInstanceOf(IllegalStateException.class).hasMessageContaining("cannot read");
    verify(lab).cleanup();
  }

  private FileExperimentResultStore store() {
    return new FileExperimentResultStore(root, clock);
  }

  private static ru.nsu.replicarecovery.model.ExperimentSpec spec() {
    return new ExperimentSpecLoader().load("""
        schemaVersion: 1
        name: restart-test
        timeout: 30m
        slo: {produceP99Ms: 100, maxErrorRate: 0.01}
        load: {mode: CONSTANT, messageSizeBytes: 1024, schedule: [{duration: 30s, requestsPerSecond: 100}]}
        recovery: {throttlePolicy: AIMD, initialThrottleMiBps: 20, minThrottleMiBps: 1, maxThrottleMiBps: 100}
        resources: {profile: normal}
        fault: {action: STOP_BROKER, target: broker-2, at: 1s}
        """);
  }
}
