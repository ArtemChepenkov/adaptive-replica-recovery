package ru.nsu.replicarecovery.runner.results;

import java.time.Instant;

public record ExperimentMetricRow(
    Instant timestamp,
    Double produceP99Ms,
    double errorRate,
    double offeredRps,
    double achievedRps,
    long throttleBytesPerSecond,
    long remainingBytes,
    long progressBytesPerSecond,
    int criticalPartitions,
    boolean safeState,
    Double brokerCpuPercent,
    Double brokerNetworkOutBytesPerSecond,
    Double brokerDiskWriteBytesPerSecond) {
  public ExperimentMetricRow {
    if (timestamp == null || !valid(produceP99Ms) || !Double.isFinite(errorRate)
        || errorRate < 0 || errorRate > 1 || !valid(offeredRps) || !valid(achievedRps)
        || throttleBytesPerSecond < 0 || remainingBytes < 0 || progressBytesPerSecond < 0
        || criticalPartitions < 0 || !valid(brokerCpuPercent)
        || !valid(brokerNetworkOutBytesPerSecond) || !valid(brokerDiskWriteBytesPerSecond)) {
      throw new IllegalArgumentException("metric values must be finite and non-negative; errorRate in [0,1]");
    }
  }

  private static boolean valid(Double value) {
    return value == null || (Double.isFinite(value) && value >= 0);
  }
}
