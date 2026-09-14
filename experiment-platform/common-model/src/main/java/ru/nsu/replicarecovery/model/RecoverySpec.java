package ru.nsu.replicarecovery.model;

import java.time.Duration;

public record RecoverySpec(
    String throttlePolicy,
    boolean adaptiveThrottleEnabled,
    boolean riskOrderingEnabled,
    long initialThrottleMiBps,
    long minThrottleMiBps,
    long maxThrottleMiBps,
    long additiveStepMiBps,
    long maxChangeMiBps,
    double multiplicativeDecrease,
    Duration controlInterval,
    Duration metricsWindow,
    Duration staleGrace,
    long safeFallbackThrottleMiBps,
    double healthySloRatio,
    int healthyWindows,
    long minimumObservations,
    Duration cooldown,
    long progressFloorMiBps,
    long minProgressMiBps,
    int noProgressWindows,
    int requiredCopies,
    int requiredDomains) {
}

