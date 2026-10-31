package ru.nsu.replicarecovery.runner.lifecycle;

import jakarta.annotation.PreDestroy;
import java.io.FileNotFoundException;
import java.io.UncheckedIOException;
import java.nio.file.NoSuchFileException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongSupplier;
import ru.nsu.replicarecovery.model.*;
import ru.nsu.replicarecovery.runner.client.*;
import ru.nsu.replicarecovery.runner.domain.ExperimentOutcome;
import ru.nsu.replicarecovery.runner.domain.ExperimentRecord;
import ru.nsu.replicarecovery.runner.lab.*;
import ru.nsu.replicarecovery.runner.results.*;

/** One worker owns the external operations. Admission is locked only by its owning caller. */
public final class ExperimentCoordinator implements AutoCloseable {
  private final ExperimentResultStore store;
  private final LoadGeneratorClient load;
  private final CruiseControlClient cruise;
  private final PrometheusClient monitor;
  private final LabController lab;
  private final Clock clock;
  private final Duration pollInterval;
  private final Duration cleanupTimeout;
  private final LongSupplier nanos;
  private final Pause pause;
  private final boolean simulated;
  private final ReentrantLock admission = new ReentrantLock();
  private final ExecutorService worker = Executors.newSingleThreadExecutor(action -> daemon(action, "experiment-worker"));
  private final ScheduledExecutorService deadlines = Executors.newSingleThreadScheduledExecutor(action -> daemon(action, "experiment-deadline"));
  private final ConcurrentHashMap<String, CompletableFuture<ExperimentRecord>> completions = new ConcurrentHashMap<>();
  private volatile Run active;
  private volatile boolean blocked;
  private boolean closed;

  public ExperimentCoordinator(ExperimentResultStore store, LoadGeneratorClient load,
      CruiseControlClient cruise, PrometheusClient monitor, LabController lab, Clock clock,
      Duration pollInterval, Duration cleanupTimeout) {
    this(store, load, cruise, monitor, lab, clock, pollInterval, cleanupTimeout,
        System::nanoTime, duration -> TimeUnit.NANOSECONDS.sleep(duration.toNanos()), false);
  }

  public ExperimentCoordinator(ExperimentResultStore store, LoadGeneratorClient load,
      CruiseControlClient cruise, PrometheusClient monitor, LabController lab, Clock clock,
      Duration pollInterval, Duration cleanupTimeout, LongSupplier nanos, Pause pause, boolean simulated) {
    if (pollInterval.isZero() || pollInterval.isNegative() || cleanupTimeout.isZero() || cleanupTimeout.isNegative()) {
      throw new IllegalArgumentException("poll and cleanup intervals must be positive");
    }
    this.store = store; this.load = load; this.cruise = cruise; this.monitor = monitor;
    this.lab = lab; this.clock = clock; this.pollInterval = pollInterval;
    this.cleanupTimeout = cleanupTimeout; this.nanos = nanos; this.pause = pause; this.simulated = simulated;
  }

  public ExperimentRecord start(ExperimentSpec spec) {
    if (!admission.tryLock()) throw new BusyException("another experiment or reset is in progress");
    try {
      if (closed || blocked || (active != null && !active.done.isDone())) {
        throw new BusyException("experiment active, cleanup pending, or reset required");
      }
      new ExperimentSpecValidator().validate(spec);
      long timeout;
      try { timeout = spec.timeout().toNanos(); }
      catch (ArithmeticException exception) { throw new IllegalArgumentException("timeout exceeds nanosecond range", exception); }
      ExperimentRecord record = store.create(spec);
      Run run = new Run(record, nanos.getAsLong());
      active = run;
      completions.put(record.id(), run.done);
      run.deadline = deadlines.schedule(() -> {
        synchronized (run.control) {
          if (!run.cleaning && !run.done.isDone()) {
            run.timedOut = true;
            Thread owner = run.owner;
            if (owner != null) owner.interrupt();
          }
        }
      }, timeout, TimeUnit.NANOSECONDS);
      worker.execute(() -> execute(run));
      return record;
    } finally { admission.unlock(); }
  }

  public ExperimentRecord get(String id) {
    try { return store.get(id); }
    catch (UncheckedIOException exception) {
      if (exception.getCause() instanceof NoSuchFileException || exception.getCause() instanceof FileNotFoundException) {
        throw new UnknownExperimentException(id);
      }
      throw exception;
    }
  }

  public List<ExperimentRecord> list() { return store.list(); }
  public List<ExperimentMetricRow> metrics(String id) { get(id); return store.metrics(id); }
  public List<Map<String, Object>> events(String id) { get(id); return store.events(id); }
  public List<Map<String, Object>> decisions(String id) { get(id); return store.decisions(id); }
  public ExperimentSummary summary(String id) {
    if (!get(id).terminal()) throw new ResultNotReadyException();
    return store.summary(id);
  }

  public ExperimentRecord stop(String id) {
    ExperimentRecord record = get(id);
    Run run = active;
    if (!record.terminal() && run != null && run.record.id().equals(id)) {
      synchronized (run.control) {
        if (!run.cleaning && !run.done.isDone()) {
          run.stopRequested.set(true);
          Thread owner = run.owner;
          if (owner != null) owner.interrupt();
        }
      }
    }
    return get(id);
  }

  public CompletableFuture<ExperimentRecord> completion(String id) {
    return completions.containsKey(id) ? completions.get(id).copy() : CompletableFuture.completedFuture(get(id));
  }

  public CleanupResult reset() {
    if (!admission.tryLock()) throw new BusyException("reset or admission in progress");
    try {
      if (closed || (active != null && !active.done.isDone())) throw new BusyException("experiment or cleanup is active");
      CleanupResult result = cleanup(null);
      if (result.success() && active != null) {
        try {
          ExperimentRecord previous = store.get(active.record.id());
          if (!previous.terminal()) {
            store.planOutcome(previous.id(), ExperimentOutcome.FAILED, "FINALIZATION_FAILED");
            store.finish(previous.id(), result);
          }
        } catch (RuntimeException exception) {
          result = new CleanupResult(true, false, List.of("RESULT_FINALIZATION_FAILED: " + exception.getMessage()));
        }
      }
      blocked = !result.success();
      return result;
    } finally { admission.unlock(); }
  }

  private void execute(Run run) {
    synchronized (run.control) { run.owner = Thread.currentThread(); }
    ExperimentOutcome outcome = ExperimentOutcome.COMPLETED;
    String reason = "RECOVERY_COMPLETED";
    try {
      if (simulated) store.appendEvent(run.record.id(), "runner", "WARNING", "SIMULATED_TEST_LAB_NO_PHYSICAL_INFRASTRUCTURE");
      checkpoint(run);
      store.transition(run.record.id(), ExperimentState.VALIDATING);
      LabValidation validation = lab.validate(run.record.spec());
      checkpoint(run);
      if (!validation.valid()) throw new IllegalStateException("LAB_VALIDATION_FAILED: " + validation.errors());
      store.transition(run.record.id(), ExperimentState.PREPARING);
      checkpoint(run);
      lab.prepare(run.record.spec());
      checkpoint(run);
      run.loadAttempted = true;
      LoadGeneratorClient.LoadStatus started = load.start(run.record.spec());
      run.loadId = started.runId();
      checkLoad(run, started);
      checkpoint(run);
      store.transition(run.record.id(), ExperimentState.WARMING_UP);
      long warmingAt = nanos.getAsLong();
      while (nanos.getAsLong() - warmingAt < run.record.spec().fault().at().toNanos()) {
        checkpoint(run);
        checkLoad(run, load.status());
        long remaining = run.record.spec().fault().at().toNanos() - (nanos.getAsLong() - warmingAt);
        pause.await(Duration.ofNanos(Math.min(pollInterval.toNanos(), Math.max(1, remaining))));
      }
      checkpoint(run);
      store.transition(run.record.id(), ExperimentState.RUNNING);
      checkpoint(run);
      Instant faultAt = clock.instant();
      lab.injectFault(run.record.spec().fault());
      store.recordTimings(run.record.id(), faultAt, null);
      checkpoint(run);
      Instant recoveryAt = clock.instant();
      store.recordTimings(run.record.id(), faultAt, recoveryAt);
      run.recoveryAttempted = true;
      String task = cruise.startRecovery(run.record.spec());
      store.appendEvent(run.record.id(), "cruise-control", "INFO", "RECOVERY_TASK " + task);
      while (true) {
        checkpoint(run);
        checkLoad(run, load.status());
        LoadMetricsSnapshot sample = load.metrics();
        checkpoint(run);
        validateMetrics(run, sample);
        RecoveryControlStatus status = cruise.status();
        run.lastRecovery = status;
        checkpoint(run);
        if (status.failed()) throw new IllegalStateException("RECOVERY_FAILED: " + status.reason());
        if (!status.measurementAvailable()) throw new IllegalStateException("RECOVERY_TELEMETRY_UNAVAILABLE");
        PrometheusClient.ResourceMetrics resources;
        try {
          resources = monitor.current();
        } catch (RuntimeException exception) {
          checkpoint(run);
          resources = new PrometheusClient.ResourceMetrics(null, null, null);
          if (!run.monitoringWarning) {
            store.appendEvent(run.record.id(), "monitoring", "WARNING", "MONITORING_UNAVAILABLE");
            run.monitoringWarning = true;
          }
        }
        if (resources == null) resources = new PrometheusClient.ResourceMetrics(null, null, null);
        if (!run.monitoringWarning && (resources.brokerCpuPercent() == null
            || resources.brokerNetworkOutBytesPerSecond() == null || resources.brokerDiskWriteBytesPerSecond() == null)) {
          store.appendEvent(run.record.id(), "monitoring", "WARNING", "MONITORING_UNAVAILABLE");
          run.monitoringWarning = true;
        }
        checkpoint(run);
        Instant timestamp = clock.instant();
        store.appendMetric(run.record.id(), new ExperimentMetricRow(timestamp, sample.p99Ms(), sample.errorRate(),
            sample.offeredRps(), sample.achievedRps(), status.throttleBytesPerSecond(), status.remainingBytes(),
            status.progressBytesPerSecond(), status.criticalPartitions(), status.safeState(),
            resources.brokerCpuPercent(), resources.brokerNetworkOutBytesPerSecond(), resources.brokerDiskWriteBytesPerSecond()));
        Map<String, Object> decision = status.decision().isEmpty()
            ? Map.of("reason", status.reason(), "throttleBytesPerSecond", status.throttleBytesPerSecond()) : status.decision();
        if (!decision.equals(run.lastDecision)) {
          store.appendDecision(run.record.id(), decision);
          run.lastDecision = decision;
        }
        if (status.completed() && !status.active()) {
          store.recordRecoveryFinished(run.record.id(), timestamp);
          break;
        }
        pause.await(pollInterval);
      }
    } catch (Exception exception) {
      if (run.timedOut || nanos.getAsLong() - run.startedNanos >= run.record.spec().timeout().toNanos()) {
        outcome = ExperimentOutcome.FAILED; reason = "TIMEOUT";
      } else if (run.stopRequested.get()) {
        outcome = ExperimentOutcome.CANCELLED; reason = "USER_STOP";
      } else {
        outcome = ExperimentOutcome.FAILED; reason = "EXECUTION_FAILED";
      }
      try { store.appendEvent(run.record.id(), "runner", "ERROR", exception.getClass().getSimpleName() + ": " + exception.getMessage()); }
      catch (RuntimeException ignored) { /* Cleanup is mandatory even when storage is unavailable. */ }
    } finally {
      synchronized (run.control) {
        run.cleaning = true;
        if (run.deadline != null) run.deadline.cancel(false);
        Thread.interrupted();
        if (run.timedOut) { outcome = ExperimentOutcome.FAILED; reason = "TIMEOUT"; }
        else if (run.stopRequested.get()) { outcome = ExperimentOutcome.CANCELLED; reason = "USER_STOP"; }
      }
      RuntimeException persistenceFailure = null;
      try { store.planOutcome(run.record.id(), outcome, reason); }
      catch (RuntimeException exception) { persistenceFailure = exception; }
      // Stop requests and ambiguity are handled before releasing the one-experiment gate.
      CleanupResult cleanup = cleanup(run);
      try {
        if (persistenceFailure == null) {
          ExperimentRecord current = store.get(run.record.id());
          if (current.state() == ExperimentState.STOPPING) store.transition(current.id(), ExperimentState.CLEANING_UP);
          store.finish(current.id(), cleanup);
        }
      } catch (RuntimeException exception) { persistenceFailure = exception; }
      admission.lock();
      try {
        ExperimentRecord terminal = null;
        if (persistenceFailure == null) {
          try { terminal = store.get(run.record.id()); }
          catch (RuntimeException exception) { persistenceFailure = exception; }
        }
        blocked = !cleanup.success() || persistenceFailure != null;
        if (persistenceFailure == null) run.done.complete(terminal);
        else run.done.completeExceptionally(persistenceFailure);
      } finally { admission.unlock(); }
      synchronized (run.control) { run.owner = null; Thread.interrupted(); }
    }
  }

  private void checkpoint(Run run) {
    if (run.timedOut || nanos.getAsLong() - run.startedNanos >= run.record.spec().timeout().toNanos()) {
      run.timedOut = true;
      throw new RunAborted("TIMEOUT");
    }
    if (run.stopRequested.get()) throw new RunAborted("USER_STOP");
  }

  private void checkLoad(Run run, LoadGeneratorClient.LoadStatus status) {
    if (!"RUNNING".equals(status.state()) || (run.loadId != null && !run.loadId.equals(status.runId()))) {
      throw new IllegalStateException("LOAD_GENERATOR_NOT_RUNNING_OR_REPLACED: " + status.state());
    }
  }

  private void validateMetrics(Run run, LoadMetricsSnapshot sample) {
    if (sample == null || sample.generatedAt() == null || sample.controlSettings() == null
        || !sample.controlSettings().equals(new ControlSettingsSnapshot(run.record.spec().slo(), run.record.spec().recovery()))
        || sample.generatedAt().isAfter(clock.instant().plusSeconds(5))
        || Duration.between(sample.generatedAt(), clock.instant()).compareTo(run.record.spec().recovery().staleGrace()) > 0) {
      throw new IllegalStateException("LOAD_METRICS_STALE_OR_WRONG_RUN");
    }
  }

  private CleanupResult cleanup(Run run) {
    List<String> errors = new ArrayList<>();
    if (run == null || run.loadAttempted) {
      try {
        var stopped = load.stop();
        long begin = nanos.getAsLong();
        while (!stopped.cleanupComplete() || stopped.outstandingRequests() != 0) {
          if (nanos.getAsLong() - begin >= cleanupTimeout.toNanos()) throw new IllegalStateException("LOAD_CLEANUP_TIMEOUT");
          pause.await(pollInterval);
          stopped = load.status();
        }
        if (!List.of("IDLE", "STOPPED", "COMPLETED", "FAILED").contains(stopped.state())
            || (run != null && "FAILED".equals(stopped.state()))) {
          errors.add("LOAD_CLEANUP_STATE: " + stopped.state());
        }
      } catch (Exception exception) { errors.add("LOAD_STOP: " + exception.getMessage()); Thread.interrupted(); }
    }
    if (run == null || run.recoveryAttempted) {
      try {
        if (run == null || run.lastRecovery == null || run.lastRecovery.active() || !run.lastRecovery.completed()) {
          cruise.stopRecovery();
          long begin = nanos.getAsLong();
          RecoveryControlStatus status = cruise.status();
          while (status.active()) {
            if (nanos.getAsLong() - begin >= cleanupTimeout.toNanos()) throw new IllegalStateException("RECOVERY_CLEANUP_TIMEOUT");
            pause.await(pollInterval);
            status = cruise.status();
          }
        }
      } catch (Exception exception) { errors.add("RECOVERY_STOP: " + exception.getMessage()); Thread.interrupted(); }
    }
    try {
      CleanupResult result = lab.cleanup();
      if (result == null || !result.attempted()) errors.add("LAB_CLEANUP_NOT_ATTEMPTED");
      else if (!result.success()) errors.addAll(result.errors().isEmpty() ? List.of("LAB_CLEANUP_FAILED") : result.errors());
    } catch (RuntimeException exception) { errors.add("LAB_CLEANUP: " + exception.getMessage()); }
    return new CleanupResult(true, errors.isEmpty(), errors);
  }

  @PreDestroy
  @Override
  public void close() {
    Run closing;
    admission.lock();
    try {
      if (closed) return;
      closed = true;
      closing = active;
      if (closing != null) {
        synchronized (closing.control) {
          if (!closing.done.isDone() && !closing.cleaning) {
            closing.stopRequested.set(true);
            if (closing.owner != null) closing.owner.interrupt();
          }
        }
      }
    } finally { admission.unlock(); }
    worker.shutdown();
    deadlines.shutdownNow();
    if (closing != null && closing.owner != Thread.currentThread()) {
      try { closing.done.get(cleanupTimeout.toNanos(), TimeUnit.NANOSECONDS); }
      catch (InterruptedException exception) { Thread.currentThread().interrupt(); blocked = true; }
      catch (Exception exception) { blocked = true; }
    }
  }

  private static Thread daemon(Runnable action, String name) {
    Thread thread = new Thread(action, name); thread.setDaemon(true); return thread;
  }
  @FunctionalInterface public interface Pause { void await(Duration duration) throws InterruptedException; }
  public static class BusyException extends RuntimeException { public BusyException(String message) { super(message); } }
  public static class UnknownExperimentException extends RuntimeException { public UnknownExperimentException(String id) { super("unknown experiment: " + id); } }
  public static class ResultNotReadyException extends RuntimeException {
    public ResultNotReadyException() { super("experiment results are not final yet"); }
  }
  private static final class RunAborted extends RuntimeException { RunAborted(String reason) { super(reason); } }
  private static final class Run {
    final ExperimentRecord record;
    final long startedNanos;
    final AtomicBoolean stopRequested = new AtomicBoolean();
    final Object control = new Object();
    final CompletableFuture<ExperimentRecord> done = new CompletableFuture<>();
    volatile Thread owner;
    volatile boolean timedOut;
    volatile boolean cleaning;
    boolean loadAttempted;
    boolean recoveryAttempted;
    boolean monitoringWarning;
    String loadId;
    RecoveryControlStatus lastRecovery;
    Map<String, Object> lastDecision;
    ScheduledFuture<?> deadline;
    Run(ExperimentRecord record, long startedNanos) { this.record = record; this.startedNanos = startedNanos; }
  }
}
