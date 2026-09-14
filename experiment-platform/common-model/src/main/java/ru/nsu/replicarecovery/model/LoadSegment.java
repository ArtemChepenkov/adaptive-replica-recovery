package ru.nsu.replicarecovery.model;

import java.time.Duration;

public record LoadSegment(Duration duration, long requestsPerSecond) {
}

