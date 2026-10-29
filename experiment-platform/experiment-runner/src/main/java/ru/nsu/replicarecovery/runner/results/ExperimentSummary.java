package ru.nsu.replicarecovery.runner.results;

import java.time.Instant;
import ru.nsu.replicarecovery.model.ExperimentState;
import ru.nsu.replicarecovery.runner.lab.CleanupResult;

/**
 * Trace aggregates, not reconstructed request-level statistics. produceP99Ms is the
 * maximum observed window p99; errorRate is a time-weighted mean of window rates.
 * Intervals hold their left sample until the next sample or measurementEndedAt,
 * excluding cleanup. Recovery duration uses the recorded executor-completion time.
 * Risk coverage is separate from optional diagnostic-resource availability.
 * The result store, not the calculator, certifies filesComplete after writing files.
 */
public record ExperimentSummary(
    int schemaVersion,
    String experimentId,
    ExperimentState finalState,
    String reason,
    CleanupResult cleanup,
    boolean filesComplete,
    Instant createdAt,
    Instant finishedAt,
    Instant measurementEndedAt,
    double totalDurationSeconds,
    Double recoveryTimeSeconds,
    Double timeToSafeSeconds,
    Double criticalPartitionSeconds,
    double missingRiskSeconds,
    boolean riskDataComplete,
    Double produceP99Ms,
    Double errorRate,
    double sloViolationTimeSeconds,
    double sloViolationAreaMsSeconds,
    Long minThrottleBytesPerSecond,
    Long maxThrottleBytesPerSecond,
    Double meanThrottleBytesPerSecond,
    double observedDurationSeconds,
    double missingLatencySeconds,
    boolean diagnosticDataComplete,
    String p99Aggregation,
    String errorRateAggregation,
    String intervalAggregation) {

  public ExperimentSummary withFilesComplete(boolean value) {
    return new ExperimentSummary(schemaVersion, experimentId, finalState, reason, cleanup, value,
        createdAt, finishedAt, measurementEndedAt, totalDurationSeconds, recoveryTimeSeconds, timeToSafeSeconds,
        criticalPartitionSeconds, missingRiskSeconds, riskDataComplete, produceP99Ms, errorRate, sloViolationTimeSeconds,
        sloViolationAreaMsSeconds, minThrottleBytesPerSecond, maxThrottleBytesPerSecond,
        meanThrottleBytesPerSecond, observedDurationSeconds, missingLatencySeconds,
        diagnosticDataComplete, p99Aggregation, errorRateAggregation, intervalAggregation);
  }
}
