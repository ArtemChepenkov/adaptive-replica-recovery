package ru.nsu.replicarecovery.model;

public record ControlSettingsSnapshot(
    SloSpec slo,
    RecoverySpec recovery) {
}

