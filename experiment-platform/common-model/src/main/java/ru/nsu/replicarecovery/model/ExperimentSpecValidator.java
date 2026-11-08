package ru.nsu.replicarecovery.model;

import java.time.Duration;
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public final class ExperimentSpecValidator {
  public void validate(ExperimentSpec spec) {
    require(spec != null, "experiment spec must not be null");
    require(spec.schemaVersion() == 1, "Unsupported schemaVersion: " + spec.schemaVersion());
    requireNotBlank(spec.name(), "name must not be blank");
    requirePositive(spec.timeout(), "timeout must be positive");

    validateSlo(spec.slo());
    validateLoad(spec.load());
    validateRecovery(spec.recovery());
    validateResources(spec.resources());
    validateFault(spec.fault(), spec.timeout());
  }

  public static void validateSlo(SloSpec slo) {
    require(slo != null, "slo must not be null");
    require(slo.produceP99Ms() > 0, "produceP99Ms must be positive");
    require(Double.isFinite(slo.produceP99Ms()), "produceP99Ms must be finite");
    require(
        slo.maxErrorRate() >= 0 && slo.maxErrorRate() <= 1,
        "maxErrorRate must be between 0 and 1");
    requirePositive(slo.maxViolationTime(), "maxViolationTime must be positive");
    require(
        Double.isFinite(slo.maxViolationAreaMsSeconds()) && slo.maxViolationAreaMsSeconds() >= 0,
        "maxViolationAreaMsSeconds must not be negative");
  }

  public static void validateLoad(LoadSpec load) {
    require(load != null, "load must not be null");
    require(load.mode() != null, "load mode must not be null");
    require(load.messageSizeBytes() > 0, "messageSizeBytes must be positive");
    require(load.schedule() != null && !load.schedule().isEmpty(), "load schedule must not be empty");

    for (LoadSegment segment : load.schedule()) {
      require(segment != null, "load segment must not be null");
      requirePositive(segment.duration(), "load segment duration must be positive");
      require(segment.requestsPerSecond() > 0, "requestsPerSecond must be positive");
    }
  }

  public static void validateRecovery(RecoverySpec recovery) {
    require(recovery != null, "recovery must not be null");
    requireNotBlank(recovery.throttlePolicy(), "throttlePolicy must not be blank");
    require(recovery.minThrottleMiBps() > 0, "minThrottleMiBps must be positive");
    require(
        recovery.maxThrottleMiBps() >= recovery.minThrottleMiBps(),
        "maxThrottleMiBps must be greater than or equal to minThrottleMiBps");
    require(
        isWithinThrottleBounds(recovery.initialThrottleMiBps(), recovery),
        "initialThrottleMiBps must be between minThrottleMiBps and maxThrottleMiBps");
    require(recovery.additiveStepMiBps() > 0, "additiveStepMiBps must be positive");
    require(recovery.maxChangeMiBps() > 0, "maxChangeMiBps must be positive");
    require(
        recovery.multiplicativeDecrease() > 0 && recovery.multiplicativeDecrease() < 1,
        "multiplicativeDecrease must be between 0 and 1");
    requirePositive(recovery.controlInterval(), "controlInterval must be positive");
    requirePositive(recovery.metricsWindow(), "metricsWindow must be positive");
    requirePositive(recovery.staleGrace(), "staleGrace must be positive");
    require(
        isWithinThrottleBounds(recovery.safeFallbackThrottleMiBps(), recovery),
        "safeFallbackThrottleMiBps must be between minThrottleMiBps and maxThrottleMiBps");
    require(
        recovery.healthySloRatio() > 0 && recovery.healthySloRatio() <= 1,
        "healthySloRatio must be between 0 and 1");
    require(recovery.healthyWindows() > 0, "healthyWindows must be positive");
    require(recovery.minimumObservations() > 0, "minimumObservations must be positive");
    requirePositive(recovery.cooldown(), "cooldown must be positive");
    require(
        isWithinThrottleBounds(recovery.progressFloorMiBps(), recovery),
        "progressFloorMiBps must be between minThrottleMiBps and maxThrottleMiBps");
    require(recovery.minProgressMiBps() > 0, "minProgressMiBps must be positive");
    require(recovery.noProgressWindows() > 0, "noProgressWindows must be positive");
    require(recovery.requiredCopies() > 0, "requiredCopies must be positive");
    require(recovery.requiredDomains() > 0, "requiredDomains must be positive");
    require(
        recovery.requiredDomains() <= recovery.requiredCopies(),
        "requiredDomains must not exceed requiredCopies");
    RecoveryExecutionSpec execution = recovery.execution();
    if (execution != null) {
      require(execution.operation() != null, "recovery.execution.operation is required");
      if (execution.goal() != null) {
        require(execution.goal().matches("[A-Za-z][A-Za-z0-9]*"), "recovery.execution.goal must be a goal name");
      }
      if (execution.excludedTopics() != null) {
        try {
          Pattern.compile(execution.excludedTopics());
        } catch (PatternSyntaxException invalid) {
          throw new IllegalArgumentException("recovery.execution.excludedTopics must be a valid regex", invalid);
        }
      }
      require(execution.concurrentMovements() == null || execution.concurrentMovements() > 0,
          "recovery.execution.concurrentMovements must be positive");
      require(execution.maxClusterMovements() == null || execution.maxClusterMovements() > 0,
          "recovery.execution.maxClusterMovements must be positive");
      if (execution.operation() == RecoveryExecutionSpec.Operation.REMOVE_BROKER) {
        require(execution.brokerId() != null && execution.brokerId() >= 0,
            "recovery.execution.brokerId is required for REMOVE_BROKER");
        require(!recovery.adaptiveThrottleEnabled(), "adaptive throttling requires REBALANCE");
        require(execution.destinationBrokerIds() == null, "recovery.execution.destinationBrokerIds requires REBALANCE");
      } else {
        require(execution.brokerId() == null, "recovery.execution.brokerId requires REMOVE_BROKER");
        List<Integer> destinations = execution.destinationBrokerIds();
        if (destinations != null) {
          require(!destinations.isEmpty() && destinations.stream().allMatch(id -> id >= 0)
              && destinations.stream().distinct().count() == destinations.size(),
              "recovery.execution.destinationBrokerIds must contain distinct non-negative broker IDs");
        }
      }
    }
  }

  private static void validateResources(ResourceSpec resources) {
    require(resources != null, "resources must not be null");
    requireNotBlank(resources.profile(), "resource profile must not be blank");
    List<ResourceProfileChange> schedule = resources.schedule();
    require(schedule != null, "resource schedule must not be null");

    Duration previous = Duration.ZERO;
    for (ResourceProfileChange change : schedule) {
      require(change != null && change.at() != null, "resource schedule entry must not be null");
      require(
          !change.at().isNegative() && change.at().compareTo(previous) >= 0,
          "resource schedule must be ordered by non-negative offset");
      requireNotBlank(change.profile(), "scheduled resource profile must not be blank");
      previous = change.at();
    }
  }

  private static void validateFault(FaultSpec fault, Duration timeout) {
    require(fault != null, "fault must not be null");
    require(fault.action() != null, "fault action must not be null");
    requireNotBlank(fault.target(), "fault target must not be blank");
    require(fault.at() != null && !fault.at().isNegative(), "fault offset must not be negative");
    require(fault.at().compareTo(timeout) < 0, "fault offset must be before experiment timeout");
  }

  private static boolean isWithinThrottleBounds(long value, RecoverySpec recovery) {
    return value >= recovery.minThrottleMiBps() && value <= recovery.maxThrottleMiBps();
  }

  private static void requirePositive(Duration value, String message) {
    require(value != null && !value.isZero() && !value.isNegative(), message);
  }

  private static void requireNotBlank(String value, String message) {
    require(value != null && !value.isBlank(), message);
  }

  private static void require(boolean condition, String message) {
    if (!condition) {
      throw new IllegalArgumentException(message);
    }
  }
}

