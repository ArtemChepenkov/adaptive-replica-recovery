package ru.nsu.replicarecovery.model;

import java.time.Duration;

public record FaultSpec(FaultAction action, String target, Duration at) {
}

