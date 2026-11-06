package ru.nsu.replicarecovery.runner.lab;

import static org.assertj.core.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

class CommandRunnerTest {
  @TempDir Path root;

  @Test void permitsOnlyFixedScriptsAndValidatedArgumentsWithoutShellInterpolation() throws Exception {
    scripts();
    var calls = new ArrayList<List<String>>();
    var runner = new CommandRunner(root, Duration.ofSeconds(2), (arguments, working, timeout) -> {
      calls.add(arguments);
      assertThat(working).isEqualTo(root.toAbsolutePath());
      return success();
    });
    runner.run(CommandRunner.Operation.PROFILE_APPLY, "network-constrained");
    runner.run(CommandRunner.Operation.FAULT_APPLY, "ISOLATE_DOMAIN", "zone-a");
    assertThat(calls.get(0)).containsExactly("bash", root.resolve("infrastructure/scripts/apply-profile.sh").toString(),
        "apply", "network-constrained");
    assertThat(calls.get(1)).endsWith("apply", "ISOLATE_DOMAIN", "zone-a");
    assertThatThrownBy(() -> runner.run("rm", "-rf", "/")).isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Operation is not allowed");
    for (String profile : List.of("../normal", "normal;touch-x", "normal x", "$(id)", "--help")) {
      assertThatThrownBy(() -> runner.run(CommandRunner.Operation.PROFILE_APPLY, profile))
          .isInstanceOf(IllegalArgumentException.class);
    }
    assertThatThrownBy(() -> runner.run(CommandRunner.Operation.FAULT_APPLY, "BOGUS", "broker-1"))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> runner.run(CommandRunner.Operation.FAULT_APPLY, "STOP_BROKER", "../broker-1"))
        .isInstanceOf(IllegalArgumentException.class);
    assertThat(calls).hasSize(2);
  }

  @Test void rejectsMissingScriptAndMalformedOrContradictoryProtocol() {
    var runner = new CommandRunner(root, Duration.ofSeconds(2), (arguments, working, timeout) -> success());
    assertThatThrownBy(() -> runner.run(CommandRunner.Operation.HEALTH)).isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(() -> new CommandRunner.Result(0, "{\"success\":true}", "").success())
        .isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(() -> new CommandRunner.Result(0,
        "{\"success\":true,\"errors\":[]} trailing-output", "").success()).isInstanceOf(IllegalStateException.class);
    assertThat(new CommandRunner.Result(1, success().stdout(), "failed").success()).isFalse();
    assertThat(new CommandRunner.Result(0, "{\"success\":true,\"unsupported\":true,\"errors\":[]}", "").success()).isFalse();
  }

  @Test @EnabledOnOs(OS.LINUX)
  void rejectsAnEscapingParentDirectorySymlinkBeforeCallingTheExecutor(@TempDir Path external) throws Exception {
    Files.createDirectories(root.resolve("infrastructure"));
    Files.writeString(external.resolve("apply-profile.sh"), "");
    Files.createSymbolicLink(root.resolve("infrastructure/scripts"), external);
    var calls = new ArrayList<List<String>>();
    var runner = new CommandRunner(root, Duration.ofSeconds(1), (arguments, working, timeout) -> {
      calls.add(arguments); return success();
    });
    assertThatThrownBy(() -> runner.run(CommandRunner.Operation.PROFILE_APPLY, "normal"))
        .isInstanceOf(IllegalStateException.class).hasMessageContaining("escapes project root");
    assertThat(calls).isEmpty();
  }

  @Test @EnabledOnOs(OS.LINUX)
  void enforcesRealCommandDeadlineAndDrainsOutputWithoutAnUnboundedWait() throws Exception {
    scripts();
    Path script = root.resolve("infrastructure/scripts/apply-profile.sh");
    Files.writeString(script, "#!/usr/bin/env bash\nsleep 30\n");
    long started = System.nanoTime();
    var runner = new CommandRunner(root, Duration.ofMillis(200));
    assertThatThrownBy(() -> runner.run(CommandRunner.Operation.PROFILE_APPLY, "normal"))
        .isInstanceOf(IllegalStateException.class).hasMessageContaining("deadline");
    assertThat(Duration.ofNanos(System.nanoTime() - started)).isLessThan(Duration.ofSeconds(5));
  }

  private void scripts() throws Exception {
    Path directory = Files.createDirectories(root.resolve("infrastructure/scripts"));
    for (String name : List.of("apply-profile.sh", "inject-fault.sh", "cleanup-lab.sh")) Files.writeString(directory.resolve(name), "");
  }
  static CommandRunner.Result success() { return new CommandRunner.Result(0, "{\"success\":true,\"unsupported\":false,\"errors\":[],\"details\":{}}", ""); }
}
