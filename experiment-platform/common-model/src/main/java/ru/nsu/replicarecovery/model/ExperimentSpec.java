package ru.nsu.replicarecovery.model;

import java.time.Duration;

public record ExperimentSpec(
    int schemaVersion,
    String name,
    Duration timeout,
    long seed,
    SloSpec slo,
    LoadSpec load,
    RecoverySpec recovery,
    ResourceSpec resources,
    FaultSpec fault) {
}

