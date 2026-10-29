package ru.nsu.replicarecovery.runner.results;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import ru.nsu.replicarecovery.model.ExperimentSpec;
import ru.nsu.replicarecovery.model.ExperimentState;
import ru.nsu.replicarecovery.model.FaultAction;
import ru.nsu.replicarecovery.model.FaultSpec;
import ru.nsu.replicarecovery.model.LoadMode;
import ru.nsu.replicarecovery.model.LoadSegment;
import ru.nsu.replicarecovery.model.LoadSpec;
import ru.nsu.replicarecovery.model.RecoverySpec;
import ru.nsu.replicarecovery.model.ResourceSpec;
import ru.nsu.replicarecovery.model.SloSpec;
import ru.nsu.replicarecovery.runner.domain.ExperimentOutcome;
import ru.nsu.replicarecovery.runner.domain.ExperimentRecord;
import ru.nsu.replicarecovery.runner.lab.CleanupResult;

class SummaryCalculatorTest {
  private static final Instant START = Instant.parse("2026-10-28T10:00:00Z");
  private final SummaryCalculator calculator = new SummaryCalculator();

  @Test
  void integratesIrregularIntervalsWithoutCallingWindowP99AGlobalPercentile() {
    ExperimentSummary summary = calculate(record(START, START, START.plusSeconds(2), null), List.of(
        row(0, 120.0, 0.02, 10, 100, 3, false),
        row(2, 90.0, 0.005, 20, 0, 1, false)), 5);

    assertThat(summary.schemaVersion()).isEqualTo(1);
    assertThat(summary.finalState()).isEqualTo(ExperimentState.COMPLETED);
    assertThat(summary.totalDurationSeconds()).isEqualTo(5);
    assertThat(summary.recoveryTimeSeconds()).isEqualTo(2);
    assertThat(summary.timeToSafeSeconds()).isNull();
    assertThat(summary.criticalPartitionSeconds()).isEqualTo(9);
    assertThat(summary.missingRiskSeconds()).isZero();
    assertThat(summary.riskDataComplete()).isTrue();
    assertThat(summary.sloViolationTimeSeconds()).isEqualTo(2);
    assertThat(summary.sloViolationAreaMsSeconds()).isEqualTo(40);
    assertThat(summary.produceP99Ms()).isEqualTo(120);
    assertThat(summary.errorRate()).isEqualTo(0.011);
    assertThat(summary.minThrottleBytesPerSecond()).isEqualTo(10);
    assertThat(summary.maxThrottleBytesPerSecond()).isEqualTo(20);
    assertThat(summary.meanThrottleBytesPerSecond()).isEqualTo(16);
    assertThat(summary.observedDurationSeconds()).isEqualTo(5);
    assertThat(summary.p99Aggregation()).isEqualTo("MAX_WINDOW_P99");
    assertThat(summary.errorRateAggregation()).isEqualTo("TIME_WEIGHTED_WINDOW_ERROR_RATE");
  }

  @Test
  void clipsCriticalExposureAtActualFaultAndUsesObservedSafeAndRecoveryTimes() {
    ExperimentSummary summary = calculate(record(START.plusSeconds(3), START.plusSeconds(4),
            START.plusSeconds(7), null),
        List.of(row(0, 90.0, 0, 10, 0, 3, true),
            row(5, 90.0, 0, 10, 10, 1, false),
            row(7, 90.0, 0, 10, 0, 0, true)), 10);

    assertThat(summary.criticalPartitionSeconds()).isEqualTo(8);
    assertThat(summary.timeToSafeSeconds()).isEqualTo(4);
    assertThat(summary.recoveryTimeSeconds()).isEqualTo(3);
    assertThat(summary.totalDurationSeconds()).isEqualTo(10);
    assertThat(summary.missingRiskSeconds()).isZero();
    assertThat(summary.riskDataComplete()).isTrue();
  }

  @Test
  void unobservedTimeAfterFaultCannotBePresentedAsZeroExposure() {
    ExperimentSummary summary = calculate(record(START, START),
        List.of(row(5, 90.0, 0, 10, 0, 0, true)), 10);

    assertThat(summary.criticalPartitionSeconds()).isZero();
    assertThat(summary.missingRiskSeconds()).isEqualTo(5);
    assertThat(summary.riskDataComplete()).isFalse();
    assertThat(summary.timeToSafeSeconds()).isEqualTo(5);
    assertThat(summary.diagnosticDataComplete()).isTrue();

    ExperimentSummary unobserved = calculate(record(START, START), List.of(), 10);
    assertThat(unobserved.criticalPartitionSeconds()).isNull();
    assertThat(unobserved.missingRiskSeconds()).isEqualTo(10);
    assertThat(unobserved.riskDataComplete()).isFalse();
  }

  @Test
  void cleanupTailAndPostMeasurementSamplesAreExcluded() {
    ExperimentSummary summary = calculate(record(START, START, null, START.plusSeconds(5)), List.of(
        row(0, 120.0, 0.02, 10, 100, 3, false),
        row(2, 120.0, 0.02, 10, 50, 3, false),
        row(7, 1000.0, 1, 9000, 0, 1000, true)), 15);

    assertThat(summary.totalDurationSeconds()).isEqualTo(15);
    assertThat(summary.measurementEndedAt()).isEqualTo(START.plusSeconds(5));
    assertThat(summary.observedDurationSeconds()).isEqualTo(5);
    assertThat(summary.sloViolationTimeSeconds()).isEqualTo(5);
    assertThat(summary.sloViolationAreaMsSeconds()).isEqualTo(100);
    assertThat(summary.criticalPartitionSeconds()).isEqualTo(15);
    assertThat(summary.produceP99Ms()).isEqualTo(120);
    assertThat(summary.maxThrottleBytesPerSecond()).isEqualTo(10);
    assertThat(summary.timeToSafeSeconds()).isNull();
  }

  @Test
  void zeroBacklogDoesNotAssertExecutorCompletion() {
    List<ExperimentMetricRow> rows = List.of(row(0, 90.0, 0, 10, 0, 0, true));
    assertThat(calculate(record(START, START), rows, 5).recoveryTimeSeconds()).isNull();
    assertThat(calculate(record(START, START, START.plusSeconds(3), null), rows, 5)
        .recoveryTimeSeconds()).isEqualTo(3);
  }

  @Test
  void doesNotConvertMissingLatencyOrResourcesToHealthyZeroes() {
    ExperimentMetricRow missing = new ExperimentMetricRow(START, null, 0.2, 100, 80,
        10, 100, 0, 1, false, null, null, null);
    ExperimentSummary summary = calculate(record(START, START), List.of(missing), 3);

    assertThat(summary.produceP99Ms()).isNull();
    assertThat(summary.missingLatencySeconds()).isEqualTo(3);
    assertThat(summary.diagnosticDataComplete()).isFalse();
    assertThat(summary.riskDataComplete()).isTrue();
    assertThat(summary.sloViolationTimeSeconds()).isEqualTo(3);
    assertThat(summary.sloViolationAreaMsSeconds()).isEqualTo(0);
    assertThat(summary.timeToSafeSeconds()).isNull();
    assertThat(summary.recoveryTimeSeconds()).isNull();
  }

  @Test
  void emptyTraceHasNoInventedLatencyThroughputOrThrottleAggregate() {
    ExperimentSummary summary = calculate(record(null, null), List.of(), 5);

    assertThat(summary.produceP99Ms()).isNull();
    assertThat(summary.errorRate()).isNull();
    assertThat(summary.minThrottleBytesPerSecond()).isNull();
    assertThat(summary.maxThrottleBytesPerSecond()).isNull();
    assertThat(summary.meanThrottleBytesPerSecond()).isNull();
    assertThat(summary.criticalPartitionSeconds()).isNull();
    assertThat(summary.missingRiskSeconds()).isZero();
    assertThat(summary.riskDataComplete()).isFalse();
    assertThat(summary.observedDurationSeconds()).isZero();
    assertThat(summary.diagnosticDataComplete()).isFalse();
    assertThat(summary.totalDurationSeconds()).isEqualTo(5);
  }

  @Test
  void failedOrCancelledExecutionDoesNotClaimCompletedRecovery() {
    for (ExperimentOutcome outcome : List.of(ExperimentOutcome.FAILED, ExperimentOutcome.CANCELLED)) {
      ExperimentSummary summary = calculator.calculate(record(START, START),
          List.of(row(0, 90.0, 0, 10, 100, 1, false)), START.plusSeconds(5),
          outcome, "MANUAL_OR_ERROR", CleanupResult.ok());
      assertThat(summary.finalState()).isEqualTo(outcome.finalState());
      assertThat(summary.recoveryTimeSeconds()).isNull();
      assertThat(summary.reason()).isEqualTo("MANUAL_OR_ERROR");
    }
  }

  @Test
  void failedCleanupCannotBecomeCompletedAndAnUnattemptedCleanupCannotFinish() {
    ExperimentSummary summary = calculator.calculate(record(START, START), List.of(),
        START.plusSeconds(5), ExperimentOutcome.COMPLETED, "RECOVERY_COMPLETED",
        new CleanupResult(true, false, List.of("NETWORK_RESET_FAILED")));
    assertThat(summary.finalState()).isEqualTo(ExperimentState.FAILED);
    assertThat(summary.cleanup().success()).isFalse();
    assertThatIllegalArgumentException().isThrownBy(() -> calculator.calculate(
        record(START, START), List.of(), START.plusSeconds(5), ExperimentOutcome.COMPLETED,
        "DONE", new CleanupResult(false, false, List.of())));
  }

  @Test
  void rejectsAmbiguousOrInvalidRowsInsteadOfProducingMisleadingNumbers() {
    ExperimentMetricRow valid = row(1, 100.0, 0, 10, 100, 1, false);
    assertThatIllegalArgumentException().isThrownBy(() -> calculate(record(START, START),
        List.of(valid, valid), 5));
    assertThatIllegalArgumentException().isThrownBy(() -> calculate(record(START, START),
        List.of(row(2, 100.0, 0, 10, 100, 1, false), valid), 5));
    assertThatIllegalArgumentException().isThrownBy(() -> calculate(record(START, START),
        List.of(row(6, 100.0, 0, 10, 100, 1, false)), 5));
    assertThatIllegalArgumentException().isThrownBy(() -> calculate(record(START, START),
        List.of(row(0, Double.NaN, 0, 10, 100, 1, false)), 5));
    assertThatIllegalArgumentException().isThrownBy(() -> calculate(record(START, START),
        List.of(row(0, 100.0, 1.1, 10, 100, 1, false)), 5));
  }

  private ExperimentSummary calculate(ExperimentRecord record, List<ExperimentMetricRow> rows, long seconds) {
    return calculator.calculate(record, rows, START.plusSeconds(seconds), ExperimentOutcome.COMPLETED,
        "RECOVERY_COMPLETED", CleanupResult.ok());
  }

  private static ExperimentMetricRow row(long seconds, Double p99, double errors, long throttle,
      long remaining, int critical, boolean safe) {
    return new ExperimentMetricRow(START.plusSeconds(seconds), p99, errors, 100, 99,
        throttle, remaining, 1, critical, safe, 30.0, 1000.0, 2000.0);
  }

  private static ExperimentRecord record(Instant faultAt, Instant recoveryAt) {
    return record(faultAt, recoveryAt, null, null);
  }

  private static ExperimentRecord record(Instant faultAt, Instant recoveryAt,
      Instant recoveryFinishedAt, Instant measurementEndedAt) {
    ExperimentSpec spec = new ExperimentSpec(1, "test", Duration.ofMinutes(30), 42,
        new SloSpec(100, 0.01, Duration.ofMinutes(2), 1000),
        new LoadSpec(LoadMode.CONSTANT, 1024, List.of(new LoadSegment(Duration.ofSeconds(30), 100))),
        new RecoverySpec("AIMD", true, true, 20, 1, 100, 5, 20, 0.7,
            Duration.ofSeconds(10), Duration.ofSeconds(60), Duration.ofSeconds(30),
            10, 0.85, 2, 100, Duration.ofSeconds(20), 5, 1, 3, 3, 3),
        new ResourceSpec("normal", List.of()), new FaultSpec(FaultAction.STOP_BROKER, "broker-2", Duration.ZERO));
    return new ExperimentRecord(1, "test-id", spec, ExperimentState.CLEANING_UP,
        ExperimentOutcome.COMPLETED, null, START, START, faultAt, recoveryAt,
        recoveryFinishedAt, measurementEndedAt);
  }
}
