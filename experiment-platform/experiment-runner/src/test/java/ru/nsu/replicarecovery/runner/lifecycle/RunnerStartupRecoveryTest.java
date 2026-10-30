package ru.nsu.replicarecovery.runner.lifecycle;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Path;
import java.time.Clock;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import ru.nsu.replicarecovery.model.ExperimentState;
import ru.nsu.replicarecovery.runner.ExperimentRunnerApplication;
import ru.nsu.replicarecovery.runner.config.ExperimentSpecLoader;
import ru.nsu.replicarecovery.runner.lab.CleanupResult;
import ru.nsu.replicarecovery.runner.lab.LabController;
import ru.nsu.replicarecovery.runner.results.FileExperimentResultStore;
import ru.nsu.replicarecovery.runner.domain.ExperimentOutcome;
import ru.nsu.replicarecovery.runner.client.LoadGeneratorClient;
import ru.nsu.replicarecovery.runner.client.CruiseControlClient;
import ru.nsu.replicarecovery.runner.client.RecoveryControlStatus;

class RunnerStartupRecoveryTest {
  @TempDir Path root;

  @Test
  void realSpringStartupRecoversThroughTheInjectedCleanupPort() {
    incomplete();
    AtomicInteger cleanups = new AtomicInteger();
    LabController lab = () -> { cleanups.incrementAndGet(); return CleanupResult.ok(); };
    try (var context = new SpringApplicationBuilder(ExperimentRunnerApplication.class)
        .web(WebApplicationType.NONE)
        .initializers(application -> {
          application.getBeanFactory().registerSingleton("testLab", lab);
          registerIdleClients(application);
        })
        .run("--runner.results-directory=" + root, "--spring.main.banner-mode=off")) {
      assertThat(cleanups.get()).isEqualTo(1);
      assertThat(context.isActive()).isTrue();
      assertThat(store().get("interrupted").state()).isEqualTo(ExperimentState.FAILED);
    }
  }

  @Test
  void restartCannotForgetTheCleanupFailureOfATerminalRun() throws Exception {
    incomplete();
    store().planOutcome("interrupted", ExperimentOutcome.FAILED, "PREVIOUS_FAILURE");
    store().finish("interrupted", new CleanupResult(true, false, java.util.List.of("previous cleanup failed")));
    String original = java.nio.file.Files.readString(root.resolve("interrupted/summary.json"));
    AtomicInteger verified = new AtomicInteger();
    LabController lab = () -> { verified.incrementAndGet(); return CleanupResult.ok(); };
    try (var context = new SpringApplicationBuilder(ExperimentRunnerApplication.class)
        .web(WebApplicationType.NONE)
        .initializers(application -> {
          registerIdleClients(application);
          application.getBeanFactory().registerSingleton("verifiedLab", lab);
        }).run("--runner.results-directory=" + root, "--spring.main.banner-mode=off")) {
      assertThat(context.isActive()).isTrue();
      assertThat(verified.get()).isEqualTo(1);
      assertThat(java.nio.file.Files.readString(root.resolve("interrupted/summary.json"))).isEqualTo(original);
    }
  }

  @Test
  void absentRealLabAdapterCannotPretendCleanupSucceeded() {
    incomplete();
    assertThatThrownBy(() -> new SpringApplicationBuilder(ExperimentRunnerApplication.class)
        .web(WebApplicationType.NONE)
        .initializers(RunnerStartupRecoveryTest::registerIdleClients)
        .run("--runner.results-directory=" + root, "--spring.main.banner-mode=off"))
        .isInstanceOf(RuntimeException.class);
    assertThat(store().get("interrupted").state()).isEqualTo(ExperimentState.FAILED);
  }

  private static void registerIdleClients(org.springframework.context.ConfigurableApplicationContext application) {
    application.getBeanFactory().registerSingleton("idleLoad", new LoadGeneratorClient() {
      public LoadStatus start(ru.nsu.replicarecovery.model.ExperimentSpec spec) { throw new UnsupportedOperationException(); }
      public ru.nsu.replicarecovery.model.LoadMetricsSnapshot metrics() { throw new UnsupportedOperationException(); }
      public LoadStatus status() { return new LoadStatus("IDLE", null, true, 0, null); }
      public LoadStatus stop() { return status(); }
    });
    application.getBeanFactory().registerSingleton("idleCruise", new CruiseControlClient() {
      public String startRecovery(ru.nsu.replicarecovery.model.ExperimentSpec spec) { throw new UnsupportedOperationException(); }
      public RecoveryControlStatus status() { return new RecoveryControlStatus(true, false, false, 0, null, null, null, null, "IDLE", java.util.Map.of()); }
      public void stopRecovery() { }
    });
  }

  private FileExperimentResultStore store() {
    return new FileExperimentResultStore(root, Clock.systemUTC());
  }

  private void incomplete() {
    store().create("interrupted", new ExperimentSpecLoader().load("""
        schemaVersion: 1
        name: startup
        timeout: 30m
        slo: {produceP99Ms: 100, maxErrorRate: 0.01}
        load: {mode: CONSTANT, messageSizeBytes: 1024, schedule: [{duration: 1s, requestsPerSecond: 100}]}
        recovery: {throttlePolicy: AIMD, initialThrottleMiBps: 20, minThrottleMiBps: 1, maxThrottleMiBps: 100}
        resources: {profile: normal}
        fault: {action: STOP_BROKER, target: broker-2, at: 1s}
        """));
  }
}
