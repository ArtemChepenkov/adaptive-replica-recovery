package ru.nsu.replicarecovery.runner.lab;

import static org.assertj.core.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class LabCleanupServiceTest {
  @TempDir Path root;
  @Test void attemptsEveryPhaseAfterFailureAndSecondVerifiedCleanupCanSucceed() throws Exception {
    Files.createDirectories(root.resolve("infrastructure/scripts"));
    Files.writeString(root.resolve("infrastructure/scripts/cleanup-lab.sh"), "");
    var fail = new AtomicBoolean(true);
    var calls = new ArrayList<String>();
    var runner = new CommandRunner(root, Duration.ofSeconds(1), (arguments, working, timeout) -> {
      String phase = arguments.get(2); calls.add(phase);
      if (fail.get() && phase.equals("reset-resources")) throw new java.io.IOException("tc unavailable");
      return CommandRunnerTest.success();
    });
    var cleanup = new LabCleanupService(runner, (severity, message) -> { });
    var first = cleanup.cleanup();
    assertThat(first.success()).isFalse();
    assertThat(first.errors()).hasSize(1);
    assertThat(calls).containsExactly("reset-resources", "start-brokers", "health", "reassignments");
    fail.set(false); calls.clear();
    assertThat(cleanup.cleanup()).isEqualTo(CleanupResult.ok());
    assertThat(calls).containsExactly("reset-resources", "start-brokers", "health", "reassignments");
  }

  @Test void persistenceOfAnEventCannotSkipPhysicalCleanupPhases() throws Exception {
    Files.createDirectories(root.resolve("infrastructure/scripts"));
    Files.writeString(root.resolve("infrastructure/scripts/cleanup-lab.sh"), "");
    var calls = new ArrayList<String>();
    var runner = new CommandRunner(root, Duration.ofSeconds(1), (arguments, working, timeout) -> {
      calls.add(arguments.get(2)); return CommandRunnerTest.success();
    });
    var cleanup = new LabCleanupService(runner, (severity, message) -> { throw new IllegalStateException("store unavailable"); });
    assertThat(cleanup.cleanup().success()).isFalse();
    assertThat(calls).hasSize(4);
  }
}
