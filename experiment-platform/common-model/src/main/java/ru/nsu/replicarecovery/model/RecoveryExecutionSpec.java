package ru.nsu.replicarecovery.model;

import java.util.List;

/** Optional Cruise Control execution parameters for a comparison scenario. */
public record RecoveryExecutionSpec(
    Operation operation,
    String goal,
    String excludedTopics,
    Integer concurrentMovements,
    Integer maxClusterMovements,
    Boolean stockConcurrencyAdjusterEnabled,
    Integer brokerId,
    List<Integer> destinationBrokerIds) {
  public RecoveryExecutionSpec {
    if (destinationBrokerIds != null) destinationBrokerIds = List.copyOf(destinationBrokerIds);
  }

  public enum Operation { REBALANCE, REMOVE_BROKER }
}
