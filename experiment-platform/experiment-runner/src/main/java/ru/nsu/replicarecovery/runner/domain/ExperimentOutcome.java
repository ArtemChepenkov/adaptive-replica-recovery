package ru.nsu.replicarecovery.runner.domain;

import ru.nsu.replicarecovery.model.ExperimentState;

public enum ExperimentOutcome {
  COMPLETED, FAILED, CANCELLED;

  public ExperimentState finalState() {
    return ExperimentState.valueOf(name());
  }
}
