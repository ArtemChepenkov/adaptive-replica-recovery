package ru.nsu.replicarecovery.model;

import java.time.Duration;
import java.util.List;

final class Fixtures {
  private Fixtures() {
  }

  static ExperimentSpec validSpec() {
    return validSpecWithSchemaVersion(1);
  }

  static ExperimentSpec validSpecWithSchemaVersion(int schemaVersion) {
    return spec(
        schemaVersion,
        "adaptive-step-load",
        Duration.ofMinutes(30),
        new SloSpec(100.0, 0.01, Duration.ofMinutes(2), 1_000.0),
        new LoadSpec(
            LoadMode.STEP,
            1_024,
            List.of(
                new LoadSegment(Duration.ofMinutes(5), 1_000),
                new LoadSegment(Duration.ofMinutes(10), 2_500))),
        recovery(20),
        new ResourceSpec(
            "normal",
            List.of(
                new ResourceProfileChange(Duration.ofMinutes(5), "network-constrained"),
                new ResourceProfileChange(Duration.ofMinutes(15), "normal"))),
        new FaultSpec(FaultAction.STOP_BROKER, "broker-2", Duration.ofMinutes(2)));
  }

  static ExperimentSpec validSpecWithInitialThrottleMiBps(long initialThrottleMiBps) {
    ExperimentSpec valid = validSpec();
    return spec(
        valid.schemaVersion(),
        valid.name(),
        valid.timeout(),
        valid.slo(),
        valid.load(),
        recovery(initialThrottleMiBps),
        valid.resources(),
        valid.fault());
  }

  static RecoverySpec recovery(long initialThrottleMiBps) {
    return new RecoverySpec(
        "AIMD",
        true,
        true,
        initialThrottleMiBps,
        1,
        100,
        5,
        20,
        0.7,
        Duration.ofSeconds(10),
        Duration.ofSeconds(60),
        Duration.ofSeconds(30),
        10,
        0.85,
        2,
        100,
        Duration.ofSeconds(20),
        5,
        1,
        3,
        3,
        3);
  }

  static ExperimentSpec spec(
      int schemaVersion,
      String name,
      Duration timeout,
      SloSpec slo,
      LoadSpec load,
      RecoverySpec recovery,
      ResourceSpec resources,
      FaultSpec fault) {
    return new ExperimentSpec(
        schemaVersion,
        name,
        timeout,
        20_260_910L,
        slo,
        load,
        recovery,
        resources,
        fault);
  }
}

