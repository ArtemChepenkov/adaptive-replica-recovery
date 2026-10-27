package ru.nsu.replicarecovery.load.core;

import ru.nsu.replicarecovery.model.LoadSpec;

public record LoadCommand(LoadSpec load, long seed) {
  public LoadCommand {
    if (load == null || load.mode() == null || load.messageSizeBytes() <= 0) {
      throw new IllegalArgumentException("load, mode and positive messageSizeBytes are required");
    }
    RateSchedule.of(load.schedule());
  }
}
