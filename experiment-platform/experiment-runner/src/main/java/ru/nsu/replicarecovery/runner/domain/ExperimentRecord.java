package ru.nsu.replicarecovery.runner.domain;

import java.time.Instant;
import ru.nsu.replicarecovery.model.ExperimentSpec;
import ru.nsu.replicarecovery.model.ExperimentState;

/** The normalized spec and durable execution metadata, stored together in experiment.json. */
public record ExperimentRecord(
    int schemaVersion,
    String id,
    ExperimentSpec spec,
    ExperimentState state,
    ExperimentOutcome plannedOutcome,
    String reason,
    Instant createdAt,
    Instant updatedAt,
    Instant faultAt,
    Instant recoveryStartedAt,
    Instant recoveryFinishedAt,
    Instant measurementEndedAt) {

  public boolean terminal() {
    return state == ExperimentState.COMPLETED || state == ExperimentState.FAILED
        || state == ExperimentState.CANCELLED;
  }
}
