package ru.nsu.replicarecovery.runner.lab;

/** Minimal restart cleanup port; Task 6 adds validation, preparation and fault injection. */
@FunctionalInterface
public interface LabController {
  CleanupResult cleanup();
}
