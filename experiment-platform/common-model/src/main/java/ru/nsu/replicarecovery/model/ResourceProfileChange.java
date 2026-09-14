package ru.nsu.replicarecovery.model;

import java.time.Duration;

public record ResourceProfileChange(Duration at, String profile) {
}

