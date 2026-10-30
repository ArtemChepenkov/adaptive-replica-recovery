package ru.nsu.replicarecovery.runner.lab;

import java.util.List;
import ru.nsu.replicarecovery.model.ExperimentSpec;
import ru.nsu.replicarecovery.model.FaultSpec;

/** Resource operations are implemented by DockerLabController in Task 9. */
@FunctionalInterface
public interface LabController {
  default LabValidation validate(ExperimentSpec spec) {
    return new LabValidation(false, List.of("LAB_ADAPTER_UNAVAILABLE"));
  }
  default void prepare(ExperimentSpec spec) {
    throw new IllegalStateException("LAB_ADAPTER_UNAVAILABLE");
  }
  default void injectFault(FaultSpec fault) {
    throw new IllegalStateException("LAB_ADAPTER_UNAVAILABLE");
  }
  CleanupResult cleanup();
}
