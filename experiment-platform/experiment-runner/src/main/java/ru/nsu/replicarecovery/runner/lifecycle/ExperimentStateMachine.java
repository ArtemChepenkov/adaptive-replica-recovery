package ru.nsu.replicarecovery.runner.lifecycle;

import java.util.Map;
import java.util.Set;
import ru.nsu.replicarecovery.model.ExperimentState;
import static ru.nsu.replicarecovery.model.ExperimentState.*;

public final class ExperimentStateMachine {
  private static final Map<ExperimentState, Set<ExperimentState>> TRANSITIONS = Map.of(
      CREATED, Set.of(VALIDATING, STOPPING, CLEANING_UP),
      VALIDATING, Set.of(PREPARING, STOPPING, CLEANING_UP),
      PREPARING, Set.of(WARMING_UP, STOPPING, CLEANING_UP),
      WARMING_UP, Set.of(RUNNING, STOPPING, CLEANING_UP),
      RUNNING, Set.of(STOPPING, CLEANING_UP),
      STOPPING, Set.of(CLEANING_UP),
      CLEANING_UP, Set.of(COMPLETED, FAILED, CANCELLED));

  public ExperimentState transition(ExperimentState from, ExperimentState to) {
    if (from == null || to == null || !TRANSITIONS.getOrDefault(from, Set.of()).contains(to)) {
      throw new IllegalArgumentException("Invalid experiment transition: " + from + " -> " + to);
    }
    return to;
  }
}
