package ru.nsu.replicarecovery.load.metrics;

import java.time.Instant;

/** Latency includes scheduling delay, from the planned send time to completion. */
public record RequestObservation(
    Instant completedAt,
    long sequence,
    long latencyMicros,
    boolean success,
    String errorClass,
    int bytes) {
  public RequestObservation {
    if (completedAt == null || sequence < 0 || latencyMicros < 0 || bytes < 0) {
      throw new IllegalArgumentException("timestamp and non-negative observation values are required");
    }
  }
}
