package ru.nsu.replicarecovery.runner.lifecycle;

import java.util.List;
import java.util.ArrayList;
import ru.nsu.replicarecovery.runner.domain.ExperimentOutcome;
import ru.nsu.replicarecovery.runner.domain.ExperimentRecord;
import ru.nsu.replicarecovery.runner.lab.CleanupResult;
import ru.nsu.replicarecovery.runner.lab.LabController;
import ru.nsu.replicarecovery.runner.results.ExperimentResultStore;

/** Runs before the application accepts experiments; cleanup must never be assumed successful. */
public final class RestartRecovery {
  private final ExperimentResultStore store;
  private final LabController lab;

  public RestartRecovery(ExperimentResultStore store, LabController lab) {
    this.store = store;
    this.lab = lab;
  }

  public void recover() {
    List<ExperimentRecord> records;
    try {
      records = store.list();
    } catch (RuntimeException exception) {
      CleanupResult result = cleanup();
      throw new IllegalStateException("cannot read persisted experiments; cleanup success=" + result.success(), exception);
    }
    boolean cleanupFailed = false;
    List<RuntimeException> persistenceFailures = new ArrayList<>();
    for (ExperimentRecord record : records) {
      if (record.terminal()) {
        continue;
      }
      RuntimeException persistenceFailure = null;
      try {
        store.appendEvent(record.id(), "runner", "WARNING", "RUNNER_RESTART_DETECTED");
        store.planOutcome(record.id(), ExperimentOutcome.FAILED, "RUNNER_RESTART");
      } catch (RuntimeException exception) {
        persistenceFailure = exception;
      }
      // Storage failures must not suppress the actual lab cleanup attempt.
      CleanupResult result = cleanup();
      if (persistenceFailure == null) {
        try {
          store.finish(record.id(), result);
        } catch (RuntimeException exception) {
          persistenceFailure = exception;
        }
      }
      if (persistenceFailure != null) {
        persistenceFailures.add(persistenceFailure);
      }
      cleanupFailed |= !result.success();
    }
    if (cleanupFailed || !persistenceFailures.isEmpty()) {
      IllegalStateException failure = new IllegalStateException(
          "restart recovery failed: cleanup unsuccessful=" + cleanupFailed + ", persistence errors=" + persistenceFailures.size());
      persistenceFailures.forEach(failure::addSuppressed);
      throw failure;
    }
  }

  private CleanupResult cleanup() {
    try {
      CleanupResult result = lab.cleanup();
      if (result == null || !result.attempted()) {
        return new CleanupResult(true, false, List.of("LAB_CLEANUP_NOT_ATTEMPTED"));
      }
      return result;
    } catch (RuntimeException exception) {
      return new CleanupResult(true, false,
          List.of(exception.getClass().getSimpleName() + ": " + exception.getMessage()));
    }
  }
}
