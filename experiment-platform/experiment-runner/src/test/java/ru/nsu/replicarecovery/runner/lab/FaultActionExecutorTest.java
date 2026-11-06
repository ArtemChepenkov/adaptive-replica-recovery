package ru.nsu.replicarecovery.runner.lab;

import static org.assertj.core.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import ru.nsu.replicarecovery.model.FaultAction;
import ru.nsu.replicarecovery.model.FaultSpec;

class FaultActionExecutorTest {
  @TempDir Path root;

  @Test void returnsObservedStopTimeRatherThanRestartCommandCompletion() throws Exception {
    var events = new ArrayList<String>();
    var executor = executor("{\"faultObservedAt\":\"2026-11-01T10:00:01Z\",\"restartCompletedAt\":\"2026-11-01T10:00:30Z\"}", events);
    Instant observed = executor.execute(new FaultSpec(FaultAction.RESTART_BROKER, "broker-4", Duration.ZERO));
    assertThat(observed).isEqualTo(Instant.parse("2026-11-01T10:00:01Z"));
    assertThat(events).anyMatch(message -> message.startsWith("FAULT_REQUESTED RESTART_BROKER broker-4"))
        .anyMatch(message -> message.contains("observedAt=2026-11-01T10:00:01Z"));
  }

  @Test void successfulExitWithoutValidObservationIsNotAConfirmedFault() throws Exception {
    for (String details : List.of("{}", "{\"faultObservedAt\":null}",
        "{\"faultObservedAt\":123}", "{\"faultObservedAt\":\"not-a-timestamp\"}")) {
      var events = new ArrayList<String>();
      var executor = executor(details, events);
      assertThatThrownBy(() -> executor.execute(new FaultSpec(FaultAction.STOP_BROKER, "broker-4", Duration.ZERO)))
          .isInstanceOf(IllegalStateException.class);
      assertThat(events).noneMatch(message -> message.startsWith("FAULT_CONFIRMED"));
    }
  }

  private FaultActionExecutor executor(String details, List<String> events) throws Exception {
    Path directory = Files.createDirectories(root.resolve("infrastructure/scripts"));
    Files.writeString(directory.resolve("inject-fault.sh"), "");
    var commands = new CommandRunner(root, Duration.ofSeconds(1), (arguments, working, timeout) -> {
      if (arguments.get(2).equals("validate")) return CommandRunnerTest.success();
      return new CommandRunner.Result(0, "{\"success\":true,\"errors\":[],\"details\":" + details + "}", "");
    });
    return new FaultActionExecutor(commands, (severity, message) -> events.add(message));
  }
}
