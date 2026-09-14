package ru.nsu.replicarecovery.model;

import java.util.List;

public record LoadSpec(
    LoadMode mode,
    int messageSizeBytes,
    List<LoadSegment> schedule) {

  public LoadSpec {
    schedule = schedule == null ? null : List.copyOf(schedule);
  }
}

