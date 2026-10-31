package ru.nsu.replicarecovery.runner.metrics;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/** Diagnostic measurements only; never a substitute for user request latency/error metrics. */
public record MonitoringSnapshot(Instant timestamp, boolean available,
    Map<Integer, Double> brokerCpuPercent, Map<Integer, Double> brokerMemoryBytes,
    Map<Integer, Double> brokerNetworkOutBytesPerSecond, Map<Integer, Double> brokerDiskWriteBytesPerSecond,
    Double underReplicatedPartitions, Double executorReplicaActions, String executionState, List<String> errors) {
  public MonitoringSnapshot {
    if (timestamp == null) throw new IllegalArgumentException("monitoring timestamp required");
    brokerCpuPercent = checked(brokerCpuPercent);
    brokerMemoryBytes = checked(brokerMemoryBytes);
    brokerNetworkOutBytesPerSecond = checked(brokerNetworkOutBytesPerSecond);
    brokerDiskWriteBytesPerSecond = checked(brokerDiskWriteBytesPerSecond);
    for (Double value : new Double[] {underReplicatedPartitions, executorReplicaActions}) {
      if (value != null && (!Double.isFinite(value) || value < 0)) throw new IllegalArgumentException("invalid diagnostic value");
    }
    errors = List.copyOf(errors);
  }
  private static Map<Integer, Double> checked(Map<Integer, Double> source) {
    source.forEach((id, value) -> {
      if (id == null || id <= 0 || value == null || !Double.isFinite(value) || value < 0) {
        throw new IllegalArgumentException("invalid per-broker diagnostic value");
      }
    });
    return Map.copyOf(source);
  }
}
