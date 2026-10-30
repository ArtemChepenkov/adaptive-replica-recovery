package ru.nsu.replicarecovery.runner.client;

import ru.nsu.replicarecovery.model.ExperimentSpec;

public interface CruiseControlClient {
  String startRecovery(ExperimentSpec spec);
  RecoveryControlStatus status();
  void stopRecovery();
}
