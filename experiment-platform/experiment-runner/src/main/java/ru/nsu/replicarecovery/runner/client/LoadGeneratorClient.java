package ru.nsu.replicarecovery.runner.client;

import ru.nsu.replicarecovery.model.ExperimentSpec;
import ru.nsu.replicarecovery.model.LoadMetricsSnapshot;

public interface LoadGeneratorClient {
  LoadStatus start(ExperimentSpec spec);
  LoadMetricsSnapshot metrics();
  LoadStatus status();
  LoadStatus stop();

  record LoadStatus(String state, String runId, boolean cleanupComplete,
      long outstandingRequests, String error) { }
}
