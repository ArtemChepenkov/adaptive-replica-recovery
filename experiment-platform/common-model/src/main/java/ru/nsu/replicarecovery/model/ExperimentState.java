package ru.nsu.replicarecovery.model;

public enum ExperimentState {
  CREATED,
  VALIDATING,
  PREPARING,
  WARMING_UP,
  RUNNING,
  STOPPING,
  CLEANING_UP,
  COMPLETED,
  FAILED,
  CANCELLED
}

