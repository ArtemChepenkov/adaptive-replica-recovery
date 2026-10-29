package ru.nsu.replicarecovery.runner.results;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import ru.nsu.replicarecovery.model.ExperimentState;
import ru.nsu.replicarecovery.runner.lab.CleanupResult;

/** A controlled trace, not a Kafka benchmark. An optional env exports its five files for inspection. */
class RunnerResultDemoTest {
  @TempDir Path temporary;

  @Test
  void writesAnInspectableControlledTrace() {
    String export = System.getenv("RUNNER_DEMO_DIRECTORY");
    Path root = export == null || export.isBlank() ? temporary : Path.of(export);
    Instant start = Instant.parse("2026-10-28T10:00:00Z");
    var createStore = new FileExperimentResultStore(root, Clock.fixed(start, ZoneOffset.UTC));
    createStore.create("simulated-task5", FileExperimentResultStoreTest.spec());
    for (var state : new ExperimentState[] {ExperimentState.VALIDATING, ExperimentState.PREPARING,
        ExperimentState.WARMING_UP, ExperimentState.RUNNING}) {
      createStore.transition("simulated-task5", state);
    }
    createStore.recordTimings("simulated-task5", start, start);
    createStore.appendEvent("simulated-task5", "test", "INFO", "SIMULATED_TRACE_NOT_A_KAFKA_EXPERIMENT");
    createStore.appendMetric("simulated-task5", FileExperimentResultStoreTest.row(start, 120.0));
    var runningStore = new FileExperimentResultStore(root, Clock.fixed(start.plusSeconds(2), ZoneOffset.UTC));
    runningStore.appendMetric("simulated-task5", new ExperimentMetricRow(start.plusSeconds(2),
        90.0, 0, 100, 100, 10, 0, 5, 0, true, null, null, null));
    runningStore.recordRecoveryFinished("simulated-task5", start.plusSeconds(2));
    runningStore.appendDecision("simulated-task5", Map.of("action", "SIMULATED_DECREASE", "throttleBytesPerSecond", 10));
    var stopStore = new FileExperimentResultStore(root, Clock.fixed(start.plusSeconds(5), ZoneOffset.UTC));
    stopStore.transition("simulated-task5", ExperimentState.STOPPING);
    stopStore.transition("simulated-task5", ExperimentState.CLEANING_UP);
    var finishStore = new FileExperimentResultStore(root, Clock.fixed(start.plusSeconds(15), ZoneOffset.UTC));
    var summary = finishStore.finish("simulated-task5", CleanupResult.ok());
    assertThat(summary.sloViolationAreaMsSeconds()).isEqualTo(40);
    assertThat(summary.criticalPartitionSeconds()).isEqualTo(6);
    assertThat(summary.timeToSafeSeconds()).isEqualTo(2);
    assertThat(summary.recoveryTimeSeconds()).isEqualTo(2);
    assertThat(summary.totalDurationSeconds()).isEqualTo(15);
    assertThat(summary.filesComplete()).isTrue();
    System.out.println("TASK5_DEMO_RESULT=" + root.resolve("simulated-task5").toAbsolutePath());
  }
}
