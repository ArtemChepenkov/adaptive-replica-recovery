package ru.nsu.replicarecovery.runner.lifecycle;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;

import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import ru.nsu.replicarecovery.model.*;
import ru.nsu.replicarecovery.runner.client.*;
import ru.nsu.replicarecovery.runner.config.ExperimentSpecLoader;
import ru.nsu.replicarecovery.runner.lab.*;
import ru.nsu.replicarecovery.runner.results.FileExperimentResultStore;
import ru.nsu.replicarecovery.runner.results.ExperimentResultStore;

class ExperimentCoordinatorTest {
  @TempDir Path root;

  @Test
  void runsInOrderAndRecordsThreePollsBeforeVerifiedCleanup() throws Exception {
    Fixture fixture = new Fixture();
    try (var coordinator = fixture.coordinator(fixture.time::advance)) {
      var accepted = coordinator.start(spec());
      var done = coordinator.completion(accepted.id()).get(5, TimeUnit.SECONDS);
      assertThat(done.state()).isEqualTo(ExperimentState.COMPLETED);
      assertThat(fixture.calls).containsSubsequence("validate", "prepare", "load.start", "fault", "recovery.start",
          "recovery.status", "load.stop", "cleanup");
      assertThat(fixture.polls.get()).isEqualTo(3);
      assertThat(fixture.cleanups.get()).isEqualTo(1);
      assertThat(coordinator.metrics(accepted.id())).hasSize(3);
      assertThat(coordinator.decisions(accepted.id())).hasSize(1);
      assertThat(fixture.store.get(accepted.id()).recoveryFinishedAt()).isNotNull();
      assertThat(fixture.store.get(accepted.id()).measurementEndedAt()).isBeforeOrEqualTo(done.updatedAt());
      assertThat(coordinator.stop(accepted.id()).state()).isEqualTo(ExperimentState.COMPLETED);
      assertThat(coordinator.summary(accepted.id())).isEqualTo(fixture.store.summary(accepted.id()));
    }
  }

  @Test
  void errorStartingRecoveryStillStopsLoadAndAttemptsCleanup() throws Exception {
    Fixture fixture = new Fixture();
    fixture.startFails = true;
    try (var coordinator = fixture.coordinator(fixture.time::advance)) {
      var id = coordinator.start(spec()).id();
      assertThat(coordinator.completion(id).get(5, TimeUnit.SECONDS).state()).isEqualTo(ExperimentState.FAILED);
      assertThat(fixture.calls).contains("load.stop", "recovery.stop", "cleanup");
      assertThat(fixture.cleanups.get()).isEqualTo(1);
    }
  }

  @Test
  void stopDuringWarmupPreventsFaultAndRecoveryAndRejectsConcurrentRuns() throws Exception {
    Fixture fixture = new Fixture();
    CountDownLatch waiting = new CountDownLatch(1);
    try (var coordinator = fixture.coordinator(duration -> {
      waiting.countDown();
      Thread.sleep(30_000);
    })) {
      var id = coordinator.start(spec()).id();
      assertThat(waiting.await(5, TimeUnit.SECONDS)).isTrue();
      assertThatThrownBy(() -> coordinator.summary(id)).isInstanceOf(ExperimentCoordinator.ResultNotReadyException.class);
      assertThatThrownBy(() -> coordinator.start(spec())).isInstanceOf(ExperimentCoordinator.BusyException.class);
      coordinator.stop(id);
      assertThat(coordinator.completion(id).get(5, TimeUnit.SECONDS).state()).isEqualTo(ExperimentState.CANCELLED);
      assertThat(fixture.calls).doesNotContain("fault", "recovery.start");
      assertThat(fixture.cleanups.get()).isEqualTo(1);
    }
  }

  @Test
  void timeoutAndFailedCleanupBlockTheNextRunUntilSuccessfulReset() throws Exception {
    Fixture fixture = new Fixture();
    fixture.neverCompletes = true;
    fixture.cleanupFails = true;
    try (var coordinator = fixture.coordinator(fixture.time::advance)) {
      var id = coordinator.start(spec()).id();
      assertThat(coordinator.completion(id).get(5, TimeUnit.SECONDS).state()).isEqualTo(ExperimentState.FAILED);
      assertThat(fixture.store.get(id).reason()).isEqualTo("TIMEOUT");
      assertThatThrownBy(() -> coordinator.start(spec())).isInstanceOf(ExperimentCoordinator.BusyException.class);
      fixture.cleanupFails = false;
      assertThat(coordinator.reset().success()).isTrue();
      fixture.neverCompletes = false;
      var next = coordinator.start(spec()).id();
      assertThat(coordinator.completion(next).get(5, TimeUnit.SECONDS).terminal()).isTrue();
    }
  }

  @Test
  void resetMustPersistThePreviousFailureBeforeAllowingANewRun() throws Exception {
    Fixture fixture = new Fixture();
    var resultStore = spy(fixture.store);
    doThrow(new IllegalStateException("one-time storage failure")).doCallRealMethod()
        .when(resultStore).finish(anyString(), any(CleanupResult.class));
    try (var coordinator = fixture.coordinator(fixture.time::advance, resultStore)) {
      var id = coordinator.start(spec()).id();
      assertThatThrownBy(() -> coordinator.completion(id).get(5, TimeUnit.SECONDS))
          .isInstanceOf(java.util.concurrent.ExecutionException.class);
      assertThat(fixture.store.get(id).terminal()).isFalse();
      assertThatThrownBy(() -> coordinator.start(spec())).isInstanceOf(ExperimentCoordinator.BusyException.class);
      assertThat(coordinator.reset().success()).isTrue();
      assertThat(fixture.store.get(id).state()).isEqualTo(ExperimentState.FAILED);
      var next = coordinator.start(spec()).id();
      assertThat(coordinator.completion(next).get(5, TimeUnit.SECONDS).state()).isEqualTo(ExperimentState.COMPLETED);
    }
  }

  @Test
  void closeWaitsForCleanupInsteadOfAbandoningTheDaemonWorker() throws Exception {
    Fixture fixture = new Fixture();
    CountDownLatch waiting = new CountDownLatch(1);
    var coordinator = fixture.coordinator(duration -> {
      waiting.countDown();
      Thread.sleep(30_000);
    });
    var id = coordinator.start(spec()).id();
    assertThat(waiting.await(5, TimeUnit.SECONDS)).isTrue();
    coordinator.close();
    assertThat(coordinator.completion(id)).isCompleted();
    assertThat(fixture.store.get(id).state()).isEqualTo(ExperimentState.CANCELLED);
    assertThat(fixture.cleanups.get()).isEqualTo(1);
  }

  @Test
  void resetAcceptsAHistoricallyFailedButPhysicallyStoppedGenerator() throws Exception {
    Fixture fixture = new Fixture();
    fixture.loadTerminalFailed = true;
    try (var coordinator = fixture.coordinator(fixture.time::advance)) {
      var first = coordinator.start(spec()).id();
      assertThat(coordinator.completion(first).get(5, TimeUnit.SECONDS).state()).isEqualTo(ExperimentState.FAILED);
      assertThatThrownBy(() -> coordinator.start(spec())).isInstanceOf(ExperimentCoordinator.BusyException.class);
      assertThat(coordinator.reset().success()).isTrue();
      fixture.loadTerminalFailed = false;
      var second = coordinator.start(spec()).id();
      assertThat(coordinator.completion(second).get(5, TimeUnit.SECONDS).state()).isEqualTo(ExperimentState.COMPLETED);
    }
  }

  @Test
  void realDeadlineInterruptsTheWorkerEvenWhenItsVirtualClockDoesNotAdvance() throws Exception {
    Fixture fixture = new Fixture();
    var base = spec();
    var shortSpec = new ExperimentSpec(base.schemaVersion(), base.name(), Duration.ofSeconds(1), base.seed(),
        base.slo(), base.load(), base.recovery(), base.resources(),
        new FaultSpec(base.fault().action(), base.fault().target(), Duration.ofMillis(500)));
    try (var coordinator = fixture.coordinator(duration -> Thread.sleep(30_000))) {
      var id = coordinator.start(shortSpec).id();
      assertThat(coordinator.completion(id).get(5, TimeUnit.SECONDS).state()).isEqualTo(ExperimentState.FAILED);
      assertThat(fixture.store.get(id).reason()).isEqualTo("TIMEOUT");
      assertThat(fixture.calls).doesNotContain("fault", "recovery.start");
      assertThat(fixture.cleanups.get()).isEqualTo(1);
    }
  }

  @Test
  void missingMonitoringIsRecordedWithoutInventingResourceZeros() throws Exception {
    Fixture fixture = new Fixture();
    fixture.monitoringFails = true;
    try (var coordinator = fixture.coordinator(fixture.time::advance)) {
      var id = coordinator.start(spec()).id();
      assertThat(coordinator.completion(id).get(5, TimeUnit.SECONDS).state()).isEqualTo(ExperimentState.COMPLETED);
      assertThat(coordinator.metrics(id)).allSatisfy(row -> assertThat(row.brokerCpuPercent()).isNull());
      assertThat(coordinator.events(id).toString()).contains("MONITORING_UNAVAILABLE");
    }
  }

  static ExperimentSpec spec() {
    return new ExperimentSpecLoader().load("""
        schemaVersion: 1
        name: coordinator-test
        timeout: 10s
        slo: {produceP99Ms: 100, maxErrorRate: 0.01}
        load: {mode: CONSTANT, messageSizeBytes: 1024, schedule: [{duration: 30s, requestsPerSecond: 100}]}
        recovery: {throttlePolicy: AIMD, initialThrottleMiBps: 20, minThrottleMiBps: 1, maxThrottleMiBps: 100}
        resources: {profile: normal}
        fault: {action: STOP_BROKER, target: broker-2, at: 2s}
        """);
  }

  private final class Fixture {
    final VirtualClock time = new VirtualClock();
    final FileExperimentResultStore store = new FileExperimentResultStore(root, time);
    final List<String> calls = new java.util.concurrent.CopyOnWriteArrayList<>();
    final AtomicInteger polls = new AtomicInteger();
    final AtomicInteger cleanups = new AtomicInteger();
    volatile boolean loadRunning;
    volatile boolean recoveryActive;
    boolean startFails;
    boolean neverCompletes;
    boolean cleanupFails;
    boolean monitoringFails;
    boolean loadTerminalFailed;
    final ExperimentSpec spec = spec();

    final LoadGeneratorClient load = new LoadGeneratorClient() {
      public LoadStatus start(ExperimentSpec ignored) { calls.add("load.start"); loadRunning = true; polls.set(0); return status(); }
      public LoadStatus status() { return new LoadStatus(loadRunning ? "RUNNING" : loadTerminalFailed ? "FAILED" : "STOPPED", "load-1", !loadRunning, 0, null); }
      public LoadStatus stop() { calls.add("load.stop"); loadRunning = false; return status(); }
      public LoadMetricsSnapshot metrics() {
        return new LoadMetricsSnapshot(time.instant().minusSeconds(60), time.instant(), time.instant(), 100,
            10.0, 50.0, 90.0, 0, 0, 100, 100, 102400, 0,
            new ControlSettingsSnapshot(spec.slo(), spec.recovery()));
      }
    };
    final CruiseControlClient cruise = new CruiseControlClient() {
      public String startRecovery(ExperimentSpec ignored) {
        calls.add("recovery.start");
        if (startFails) throw new IllegalStateException("ambiguous start");
        recoveryActive = true;
        return "task-1";
      }
      public RecoveryControlStatus status() {
        calls.add("recovery.status");
        if (recoveryActive && !neverCompletes && polls.incrementAndGet() >= 3) recoveryActive = false;
        return new RecoveryControlStatus(!recoveryActive, false, recoveryActive, 20,
            recoveryActive ? 100L : 0L, 1L, recoveryActive ? 1 : 0, !recoveryActive,
            "TEST", Map.of("action", "HOLD"));
      }
      public void stopRecovery() { calls.add("recovery.stop"); recoveryActive = false; }
    };
    final LabController lab = new LabController() {
      public LabValidation validate(ExperimentSpec ignored) { calls.add("validate"); return LabValidation.ok(); }
      public void prepare(ExperimentSpec ignored) { calls.add("prepare"); }
      public void injectFault(FaultSpec ignored) { calls.add("fault"); }
      public CleanupResult cleanup() {
        calls.add("cleanup"); cleanups.incrementAndGet();
        return new CleanupResult(true, !cleanupFails, cleanupFails ? List.of("CLEANUP_TEST_FAILURE") : List.of());
      }
    };

    ExperimentCoordinator coordinator(ExperimentCoordinator.Pause pause) {
      return coordinator(pause, store);
    }

    ExperimentCoordinator coordinator(ExperimentCoordinator.Pause pause, ExperimentResultStore results) {
      return new ExperimentCoordinator(results, load, cruise, () -> {
        if (monitoringFails) throw new IllegalStateException("offline");
        return new PrometheusClient.ResourceMetrics(10.0, 100.0, 200.0);
      }, lab, time, Duration.ofSeconds(1), Duration.ofSeconds(3), time.nanos::get, pause, false);
    }
  }

  private static final class VirtualClock extends Clock {
    final AtomicLong nanos = new AtomicLong();
    public ZoneId getZone() { return ZoneOffset.UTC; }
    public Clock withZone(ZoneId zone) { return this; }
    public Instant instant() { return Instant.parse("2026-10-29T10:00:00Z").plusNanos(nanos.get()); }
    void advance(Duration duration) { nanos.addAndGet(duration.toNanos()); }
  }
}
