package ru.nsu.replicarecovery.model;

import java.util.List;

public record ResourceSpec(
    String profile,
    List<ResourceProfileChange> schedule) {

  public ResourceSpec {
    schedule = schedule == null ? null : List.copyOf(schedule);
  }
}

