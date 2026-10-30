package ru.nsu.replicarecovery.runner.client;

import java.util.Map;

/** Missing extension telemetry remains null and is not fabricated as zero or safe. */
public record RecoveryControlStatus(boolean completed, boolean failed, boolean active,
    long throttleBytesPerSecond, Long remainingBytes, Long progressBytesPerSecond,
    Integer criticalPartitions, Boolean safeState, String reason, Map<String, Object> decision) {
  public RecoveryControlStatus { decision = decision == null ? Map.of() : Map.copyOf(decision); }
  public boolean measurementAvailable() {
    return remainingBytes != null && progressBytesPerSecond != null
        && criticalPartitions != null && safeState != null;
  }
}
