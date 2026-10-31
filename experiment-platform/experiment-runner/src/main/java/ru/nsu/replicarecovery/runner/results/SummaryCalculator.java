package ru.nsu.replicarecovery.runner.results;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import ru.nsu.replicarecovery.runner.domain.ExperimentOutcome;
import ru.nsu.replicarecovery.runner.domain.ExperimentRecord;
import ru.nsu.replicarecovery.runner.lab.CleanupResult;

/**
 * Uses left-held poll samples; an absent measurement is never silently a healthy zero.
 * Recovery time uses an explicit executor-completion timestamp; zero backlog does
 * not certify completion. Time-to-safe is the first observed safe state after the
 * actual fault and has poll-resolution accuracy. Cleanup is excluded from trace integrals.
 */
public final class SummaryCalculator {
  public ExperimentSummary calculate(ExperimentRecord record, List<ExperimentMetricRow> rows,
      Instant finishedAt, ExperimentOutcome outcome, String reason, CleanupResult cleanup) {
    require(record != null && record.spec() != null && record.spec().slo() != null && record.createdAt() != null,
        "experiment record, spec and createdAt are required");
    require(finishedAt != null && !finishedAt.isBefore(record.createdAt()),
        "finishedAt must not precede createdAt");
    require(outcome != null && cleanup != null && cleanup.attempted(),
        "an outcome and an attempted cleanup are required");
    require(reason != null && !reason.isBlank(), "completion reason is required");
    require(rows != null, "metric rows are required");
    requireValidTiming(record.faultAt(), record.createdAt(), finishedAt, "faultAt");
    requireValidTiming(record.recoveryStartedAt(), record.createdAt(), finishedAt, "recoveryStartedAt");
    requireValidTiming(record.recoveryFinishedAt(), record.createdAt(), finishedAt, "recoveryFinishedAt");
    requireValidTiming(record.measurementEndedAt(), record.createdAt(), finishedAt, "measurementEndedAt");
    Instant measurementEnd = record.measurementEndedAt() == null ? finishedAt : record.measurementEndedAt();
    require(record.recoveryFinishedAt() == null || (record.recoveryStartedAt() != null
        && !record.recoveryFinishedAt().isBefore(record.recoveryStartedAt())),
        "recoveryFinishedAt must follow recoveryStartedAt");

    Double maximumP99 = null;
    Double recoveryTime = record.recoveryFinishedAt() == null ? null
        : seconds(record.recoveryStartedAt(), record.recoveryFinishedAt());
    Double safeTime = null;
    Long minimumThrottle = null;
    Long maximumThrottle = null;
    double throttleIntegral = 0;
    double errorIntegral = 0;
    double observedSeconds = 0;
    double violationSeconds = 0;
    double violationArea = 0;
    double missingLatencySeconds = 0;
    double criticalSeconds = 0;
    boolean diagnosticsComplete = true;
    int measuredRows = 0;
    Instant previous = null;

    for (int index = 0; index < rows.size(); index++) {
      ExperimentMetricRow row = rows.get(index);
      require(row != null, "metric row must not be null");
      require(!row.timestamp().isBefore(record.createdAt()) && !row.timestamp().isAfter(finishedAt),
          "metric timestamp must be within the experiment lifetime");
      require(previous == null || row.timestamp().isAfter(previous),
          "metric timestamps must be strictly increasing");
      require(index + 1 == rows.size() || rows.get(index + 1) != null, "metric row must not be null");
      Instant intervalEnd = index + 1 < rows.size() ? rows.get(index + 1).timestamp() : finishedAt;
      require(intervalEnd != null && !intervalEnd.isBefore(row.timestamp()),
          "metric timestamps must be ordered");
      previous = row.timestamp();
      if (row.timestamp().isAfter(measurementEnd)) {
        continue;
      }
      measuredRows++;
      if (intervalEnd.isAfter(measurementEnd)) {
        intervalEnd = measurementEnd;
      }
      double interval = seconds(row.timestamp(), intervalEnd);
      observedSeconds += interval;
      errorIntegral += row.errorRate() * interval;
      throttleIntegral += row.throttleBytesPerSecond() * interval;
      minimumThrottle = minimumThrottle == null ? row.throttleBytesPerSecond()
          : Math.min(minimumThrottle, row.throttleBytesPerSecond());
      maximumThrottle = maximumThrottle == null ? row.throttleBytesPerSecond()
          : Math.max(maximumThrottle, row.throttleBytesPerSecond());

      if (row.produceP99Ms() == null) {
        missingLatencySeconds += interval;
      } else {
        maximumP99 = maximumP99 == null ? row.produceP99Ms()
            : Math.max(maximumP99, row.produceP99Ms());
        violationArea += Math.max(0, row.produceP99Ms() - record.spec().slo().produceP99Ms()) * interval;
      }
      if ((row.produceP99Ms() != null && row.produceP99Ms() > record.spec().slo().produceP99Ms())
          || row.errorRate() > record.spec().slo().maxErrorRate()) {
        violationSeconds += interval;
      }
      if (record.faultAt() != null) {
        Instant riskStart = row.timestamp().isBefore(record.faultAt()) ? record.faultAt() : row.timestamp();
        if (intervalEnd.isAfter(riskStart)) {
          criticalSeconds += row.criticalPartitions() * seconds(riskStart, intervalEnd);
        }
        if (safeTime == null && row.safeState() && !row.timestamp().isBefore(record.faultAt())) {
          safeTime = seconds(record.faultAt(), row.timestamp());
        }
      }
      diagnosticsComplete &= row.brokerCpuPercent() != null
          && row.brokerNetworkOutBytesPerSecond() != null && row.brokerDiskWriteBytesPerSecond() != null
          && (row.monitoring() == null || row.monitoring().available());
    }

    requireFinite(throttleIntegral, "throttle integral");
    requireFinite(errorIntegral, "error integral");
    requireFinite(violationArea, "SLO violation area");
    requireFinite(criticalSeconds, "critical partition-seconds");
    double missingRiskSeconds = 0;
    boolean riskDataComplete = false;
    if (record.faultAt() != null) {
      Instant firstCoveredAt = measuredRows == 0 ? measurementEnd : rows.get(0).timestamp();
      missingRiskSeconds = Math.max(0, seconds(record.faultAt(), firstCoveredAt));
      riskDataComplete = measuredRows > 0 && missingRiskSeconds == 0;
    }
    ExperimentOutcome finalOutcome = cleanup.success() ? outcome : ExperimentOutcome.FAILED;
    String finalReason = !cleanup.success() && outcome != ExperimentOutcome.FAILED ? "CLEANUP_FAILED" : reason;
    return new ExperimentSummary(1, record.id(), finalOutcome.finalState(), finalReason, cleanup, false,
        record.createdAt(), finishedAt, measurementEnd, seconds(record.createdAt(), finishedAt), recoveryTime, safeTime,
        record.faultAt() == null || measuredRows == 0 ? null : criticalSeconds,
        missingRiskSeconds, riskDataComplete, maximumP99,
        observedSeconds == 0 ? null : errorIntegral / observedSeconds,
        violationSeconds, violationArea, minimumThrottle, maximumThrottle,
        observedSeconds == 0 ? null : throttleIntegral / observedSeconds,
        observedSeconds, missingLatencySeconds, measuredRows > 0 && diagnosticsComplete,
        "MAX_WINDOW_P99", "TIME_WEIGHTED_WINDOW_ERROR_RATE", "LEFT_HOLD_TO_NEXT_SAMPLE_OR_MEASUREMENT_END");
  }

  private static void requireValidTiming(Instant value, Instant startedAt, Instant finishedAt, String field) {
    require(value == null || (!value.isBefore(startedAt) && !value.isAfter(finishedAt)),
        field + " must be within the experiment lifetime");
  }

  private static void requireFinite(double value, String field) {
    require(Double.isFinite(value), field + " must be finite");
  }

  private static double seconds(Instant start, Instant end) {
    Duration duration = Duration.between(start, end);
    return duration.getSeconds() + duration.getNano() / 1_000_000_000.0;
  }

  private static void require(boolean condition, String message) {
    if (!condition) {
      throw new IllegalArgumentException(message);
    }
  }
}
