package ru.nsu.replicarecovery.runner.lab;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.io.TempDir;
import ru.nsu.replicarecovery.runner.config.ExperimentSpecLoader;

/**
 * Opt-in physical adapter checkpoint: changes the owned lab, stops broker-4 and restores it.
 * It does not start Load Generator or Cruise Control recovery and measures no user SLO.
 */
@EnabledIfEnvironmentVariable(named = "RUN_LAB_INTEGRATION", matches = "true")
class DockerLabControllerIntegrationTest {
  @TempDir Path temporary;

  @Test
  @Timeout(value = 10, unit = TimeUnit.MINUTES)
  void appliesScheduledProfileConfirmsPhysicalFaultAndVerifiesTwoCleanups() throws Exception {
    String configuredRoot = System.getenv("LAB_PROJECT_ROOT");
    assertThat(configuredRoot).as("Explicit LAB_PROJECT_ROOT is required for the physical checkpoint").isNotBlank();
    Path root = Path.of(configuredRoot).toRealPath();
    assertThat(root.resolve("infrastructure/docker-compose.yml")).isRegularFile();
    assertThat(System.getenv().getOrDefault("COMPOSE_PROJECT_NAME", "replica-recovery"))
        .as("This checkpoint is restricted to the named diploma lab")
        .isEqualTo("replica-recovery");
    String profile = System.getenv().getOrDefault("LAB_TEST_PROFILE", "network-constrained");
    assertThat(profile).matches("[a-z0-9][a-z0-9-]{0,63}");

    Path evidence = evidence(root);
    Path journal = evidence.resolve("events.jsonl");
    Files.createFile(journal);
    var json = new ObjectMapper().findAndRegisterModules()
        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
        .disable(SerializationFeature.WRITE_DURATIONS_AS_TIMESTAMPS);
    var messages = new ArrayList<String>();
    String id = "physical-adapter-" + UUID.randomUUID();
    // Keep the sequence deterministic while all script operations remain physical.
    var timeline = new AtomicLong();
    var controller = new DockerLabController(new CommandRunner(root, Duration.ofMinutes(3)), timeline::get);
    controller.beginExperiment(id, (severity, message) -> {
      messages.add(message);
      try {
        Files.writeString(journal, json.writeValueAsString(Map.of("timestamp", Instant.now(), "source", "lab",
            "severity", severity, "message", message)) + "\n", StandardCharsets.UTF_8, StandardOpenOption.APPEND);
      } catch (IOException exception) { throw new UncheckedIOException(exception); }
    });
    var spec = new ExperimentSpecLoader().load("""
        schemaVersion: 1
        name: physical-adapter-only-integration
        timeout: 10m
        slo: {produceP99Ms: 100, maxErrorRate: 0.01}
        load: {mode: CONSTANT, messageSizeBytes: 1024, schedule: [{duration: 10m, requestsPerSecond: 100}]}
        recovery: {throttlePolicy: AIMD, initialThrottleMiBps: 20, minThrottleMiBps: 1, maxThrottleMiBps: 100}
        resources:
          profile: normal
          schedule: [{at: 0s, profile: %s}, {at: 1s, profile: normal}]
        fault: {action: STOP_BROKER, target: broker-4, at: 2s}
        """.formatted(profile));
    Files.writeString(evidence.resolve("spec.json"), json.writerWithDefaultPrettyPrinter().writeValueAsString(spec),
        StandardCharsets.UTF_8, StandardOpenOption.CREATE_NEW);

    String phase = "validate";
    Throwable bodyFailure = null;
    boolean bodyCompleted = false;
    Instant faultObservedAt = null;
    var cleanups = new ArrayList<CleanupResult>();
    try {
      var validation = controller.validate(spec);
      assertThat(validation.valid()).as("All profiles and the physical fault must be admitted: %s", validation.errors()).isTrue();
      phase = "prepare";
      controller.prepare(spec);
      // Marking the resource timeline does not start any producer or recovery process.
      controller.loadStarted();
      phase = "scheduled-profile";
      controller.tick();
      phase = "stop-broker";
      Instant faultRequestedAt = Instant.now();
      controller.injectFault(spec.fault());
      faultObservedAt = controller.observedFaultAt();
      assertThat(faultObservedAt).isNotNull().isAfterOrEqualTo(faultRequestedAt).isBeforeOrEqualTo(Instant.now());
      phase = "normal-profile-while-own-broker-is-stopped";
      timeline.set(Duration.ofSeconds(1).toNanos());
      controller.tick();
      assertThat(messages).anyMatch(message -> message.startsWith("RESOURCE_PROFILE_APPLIED " + profile));
      assertThat(messages).anyMatch(message -> message.startsWith("FAULT_CONFIRMED STOP_BROKER broker-4"));
      assertThat(messages.stream().filter(message -> message.startsWith("RESOURCE_PROFILE_APPLIED normal")))
          .as("The normal schedule must run after the initial normal preparation").hasSizeGreaterThanOrEqualTo(2);
      bodyCompleted = true;
    } catch (RuntimeException | AssertionError exception) {
      bodyFailure = exception;
      throw exception;
    } finally {
      boolean interrupted = Thread.interrupted();
      try {
        // The second attempt must execute even if the first cleanup reports an error.
        for (int attempt = 0; attempt < 2; attempt++) {
          try { cleanups.add(controller.cleanup()); }
          catch (RuntimeException exception) {
            cleanups.add(new CleanupResult(true, false, List.of(exception.toString())));
          }
        }
        var result = new LinkedHashMap<String, Object>();
        result.put("experimentId", id);
        result.put("test", "PHYSICAL_ADAPTER_ONLY_NO_LOAD_OR_RECOVERY");
        result.put("timeline", "CONTROLLED_MONOTONIC_FOR_OPERATION_SEQUENCE");
        result.put("profile", profile);
        result.put("bodyCompleted", bodyCompleted);
        result.put("faultObservedAt", faultObservedAt);
        result.put("lastPhase", phase);
        result.put("failure", bodyFailure == null ? null : bodyFailure.toString());
        result.put("cleanups", cleanups);
        result.put("success", bodyCompleted && cleanups.stream().allMatch(CleanupResult::success));
        Files.writeString(evidence.resolve("result.json"), json.writerWithDefaultPrettyPrinter().writeValueAsString(result),
            StandardCharsets.UTF_8, StandardOpenOption.CREATE_NEW);
        if (bodyFailure == null) {
          assertThat(cleanups).hasSize(2).allSatisfy(cleanup -> assertThat(cleanup.success()).as("%s", cleanup.errors()).isTrue());
        } else if (cleanups.stream().anyMatch(cleanup -> !cleanup.success())) {
          bodyFailure.addSuppressed(new IllegalStateException("Physical cleanup is unverified: " + cleanups));
        }
      } finally {
        if (interrupted) Thread.currentThread().interrupt();
      }
    }
  }

  private Path evidence(Path root) throws IOException {
    String configured = System.getenv("LAB_INTEGRATION_EVIDENCE_DIRECTORY");
    if (configured == null || configured.isBlank()) return temporary;
    Path directory = root.resolve(configured).toAbsolutePath().normalize();
    if (!directory.startsWith(root)) throw new IllegalArgumentException("Lab evidence must remain inside the workspace");
    Files.createDirectories(directory);
    if (!directory.toRealPath().startsWith(root)) throw new IllegalArgumentException("Lab evidence symlink escapes workspace");
    return directory;
  }
}
