package ru.nsu.replicarecovery.runner.lab;

import static org.assertj.core.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import ru.nsu.replicarecovery.model.ExperimentSpec;
import ru.nsu.replicarecovery.runner.config.ExperimentSpecLoader;

class DockerLabControllerTest {
  @TempDir Path root;

  @Test void validatesEveryScheduledProfileBeforePreparingAndDoesNotAcceptUnsupportedDisk() throws Exception {
    var calls = new ArrayList<List<String>>();
    var controller = controller(calls, true, new AtomicLong());
    var validation = controller.validate(spec());
    assertThat(validation.valid()).isFalse();
    assertThat(validation.errors().toString()).contains("disk-constrained", "not calibrated");
    assertThat(calls.stream().map(command -> String.join(" ", command))).anyMatch(command -> command.endsWith("validate normal"))
        .anyMatch(command -> command.endsWith("validate network-constrained"))
        .anyMatch(command -> command.endsWith("validate disk-constrained"));
    assertThatThrownBy(() -> controller.prepare(spec())).isInstanceOf(IllegalStateException.class);
    assertThat(calls).noneMatch(command -> command.get(2).equals("apply"));
  }

  @Test void workerLifecycleRecordsObservedOperationsAndCancelsScheduleBeforeCleanup() throws Exception {
    var calls = new ArrayList<List<String>>();
    var time = new AtomicLong();
    var controller = controller(calls, false, time);
    var events = new ArrayList<String>();
    controller.beginExperiment("run-1", (severity, message) -> events.add(message));
    assertThat(controller.validate(spec()).valid()).isTrue();
    controller.prepare(spec()); controller.tick();
    controller.loadStarted();
    time.set(Duration.ofSeconds(2).toNanos()); controller.tick(); controller.tick();
    controller.injectFault(spec().fault());
    assertThat(controller.observedFaultAt()).isEqualTo(Instant.parse("2026-11-01T10:00:00Z"));
    assertThat(controller.cleanup().success()).isTrue();
    assertThat(controller.observedFaultAt()).isNull();
    int afterCleanup = calls.size();
    time.set(Duration.ofSeconds(20).toNanos()); controller.tick();
    assertThat(calls).hasSize(afterCleanup);
    assertThat(events.toString()).contains("RESOURCE_PROFILE_APPLIED normal", "RESOURCE_PROFILE_APPLIED network-constrained",
        "RESOURCE_TIMELINE_STARTED", "FAULT_REQUESTED STOP_BROKER broker-2", "FAULT_CONFIRMED STOP_BROKER broker-2");
    assertThat(calls.stream().filter(command -> command.get(2).equals("apply")
        && command.get(command.size() - 1).equals("network-constrained"))).hasSize(1);
  }

  private DockerLabController controller(List<List<String>> calls, boolean rejectDisk, AtomicLong time) throws Exception {
    Path directory = Files.createDirectories(root.resolve("infrastructure/scripts"));
    for (String name : List.of("apply-profile.sh", "inject-fault.sh", "cleanup-lab.sh")) Files.writeString(directory.resolve(name), "");
    var runner = new CommandRunner(root, Duration.ofSeconds(1), (arguments, working, timeout) -> {
      calls.add(arguments);
      if (rejectDisk && arguments.get(2).equals("validate") && arguments.get(arguments.size() - 1).equals("disk-constrained")) {
        return new CommandRunner.Result(2, "{\"success\":false,\"unsupported\":true,\"errors\":[\"disk not calibrated\"],\"details\":{}}", "");
      }
      if (arguments.get(1).endsWith("inject-fault.sh") && arguments.get(2).equals("apply")) {
        return new CommandRunner.Result(0,
            "{\"success\":true,\"errors\":[],\"details\":{\"faultObservedAt\":\"2026-11-01T10:00:00Z\"}}", "");
      }
      return CommandRunnerTest.success();
    });
    return new DockerLabController(runner, time::get);
  }

  private ExperimentSpec spec() {
    return new ExperimentSpecLoader().load("""
        schemaVersion: 1
        name: lab-test
        timeout: 2m
        slo: {produceP99Ms: 100, maxErrorRate: 0.01}
        load: {mode: CONSTANT, messageSizeBytes: 1024, schedule: [{duration: 2m, requestsPerSecond: 100}]}
        recovery: {throttlePolicy: AIMD, initialThrottleMiBps: 20, minThrottleMiBps: 1, maxThrottleMiBps: 100}
        resources:
          profile: normal
          schedule: [{at: 1s, profile: network-constrained}, {at: 10s, profile: disk-constrained}]
        fault: {action: STOP_BROKER, target: broker-2, at: 2s}
        """);
  }
}
