package ru.nsu.replicarecovery.load.web;

import ru.nsu.replicarecovery.load.core.LoadCommand;
import ru.nsu.replicarecovery.model.ExperimentSpecValidator;
import ru.nsu.replicarecovery.model.LoadSpec;
import ru.nsu.replicarecovery.model.RecoverySpec;
import ru.nsu.replicarecovery.model.SloSpec;

public record LoadStartRequest(LoadSpec load, SloSpec slo, RecoverySpec recovery, long seed) {
  public void validate() {
    ExperimentSpecValidator.validateLoad(load);
    ExperimentSpecValidator.validateSlo(slo);
    ExperimentSpecValidator.validateRecovery(recovery);
    new LoadCommand(load, seed);
    if (recovery.metricsWindow().toMillis() <= 0) {
      throw new IllegalArgumentException("metricsWindow must be at least one millisecond");
    }
  }
}
