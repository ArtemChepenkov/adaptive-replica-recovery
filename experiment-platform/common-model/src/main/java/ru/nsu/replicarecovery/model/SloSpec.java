package ru.nsu.replicarecovery.model;

import java.time.Duration;

public record SloSpec(
    double produceP99Ms,
    double maxErrorRate,
    Duration maxViolationTime,
    double maxViolationAreaMsSeconds) {
}

