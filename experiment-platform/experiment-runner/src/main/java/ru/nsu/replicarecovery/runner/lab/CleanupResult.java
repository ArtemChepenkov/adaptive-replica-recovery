package ru.nsu.replicarecovery.runner.lab;

import java.util.List;

public record CleanupResult(boolean attempted, boolean success, List<String> errors) {
  public CleanupResult {
    errors = List.copyOf(errors);
    if (success && (!attempted || !errors.isEmpty())) {
      throw new IllegalArgumentException("successful cleanup must be attempted without errors");
    }
  }

  public static CleanupResult ok() {
    return new CleanupResult(true, true, List.of());
  }
}
