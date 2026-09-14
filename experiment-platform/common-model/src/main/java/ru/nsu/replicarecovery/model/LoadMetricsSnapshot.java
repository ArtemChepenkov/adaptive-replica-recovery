package ru.nsu.replicarecovery.model;

import java.time.Instant;

public record LoadMetricsSnapshot(
    Instant windowStart,
    Instant windowEnd,
    Instant generatedAt,
    long requestCount,
    Double p50Ms,
    Double p95Ms,
    Double p99Ms,
    long errorCount,
    double errorRate,
    double offeredRps,
    double achievedRps,
    long bytesSent,
    long outstandingRequests,
    ControlSettingsSnapshot controlSettings) {
}

