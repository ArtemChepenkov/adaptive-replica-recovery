package ru.nsu.replicarecovery.runner.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import ru.nsu.replicarecovery.model.ExperimentSpec;

class ExperimentSpecLoaderTest {
  @TempDir Path directory;
  private final ExperimentSpecLoader loader = new ExperimentSpecLoader();

  @Test
  void loadsTheShortSpecificationAndMaterializesDocumentedDefaults() {
    ExperimentSpec spec = loader.load(shortYaml());

    assertThat(spec.schemaVersion()).isEqualTo(1);
    assertThat(spec.seed()).isEqualTo(20260910);
    assertThat(spec.timeout()).isEqualTo(Duration.ofMinutes(30));
    assertThat(spec.load().schedule().get(0).duration()).isEqualTo(Duration.ofMinutes(5));
    assertThat(spec.fault().at()).isEqualTo(Duration.ofMinutes(2));
    assertThat(spec.slo().maxViolationTime()).isEqualTo(Duration.ofMinutes(2));
    assertThat(spec.slo().maxViolationAreaMsSeconds()).isEqualTo(1000);
    assertThat(spec.resources().schedule()).isEmpty();
    var recovery = spec.recovery();
    assertThat(recovery.adaptiveThrottleEnabled()).isTrue();
    assertThat(recovery.riskOrderingEnabled()).isTrue();
    assertThat(recovery.additiveStepMiBps()).isEqualTo(5);
    assertThat(recovery.maxChangeMiBps()).isEqualTo(20);
    assertThat(recovery.multiplicativeDecrease()).isEqualTo(0.7);
    assertThat(recovery.controlInterval()).isEqualTo(Duration.ofSeconds(10));
    assertThat(recovery.metricsWindow()).isEqualTo(Duration.ofSeconds(60));
    assertThat(recovery.staleGrace()).isEqualTo(Duration.ofSeconds(30));
    assertThat(recovery.safeFallbackThrottleMiBps()).isEqualTo(20);
    assertThat(recovery.healthySloRatio()).isEqualTo(0.85);
    assertThat(recovery.healthyWindows()).isEqualTo(2);
    assertThat(recovery.minimumObservations()).isEqualTo(100);
    assertThat(recovery.cooldown()).isEqualTo(Duration.ofSeconds(20));
    assertThat(recovery.progressFloorMiBps()).isEqualTo(5);
    assertThat(recovery.minProgressMiBps()).isEqualTo(1);
    assertThat(recovery.noProgressWindows()).isEqualTo(3);
    assertThat(recovery.requiredCopies()).isEqualTo(3);
    assertThat(recovery.requiredDomains()).isEqualTo(3);
  }

  @Test
  void preservesExplicitValuesFalseFlagsAndResourceSchedule() {
    String yaml = shortYaml().replace("schemaVersion: 1", "schemaVersion: 1\nseed: 7")
        .replace("throttlePolicy: AIMD", "throttlePolicy: FIXED\n  adaptiveThrottleEnabled: false\n"
            + "  riskOrderingEnabled: false\n  safeFallbackThrottleMiBps: 12\n  additiveStepMiBps: 9")
        .replace("maxErrorRate: 0.01", "maxErrorRate: 0.01\n  maxViolationTime: PT3M\n"
            + "  maxViolationAreaMsSeconds: 1200")
        .replace("profile: normal", "profile: normal\n  schedule:\n    - at: 3m\n      profile: high-latency");

    ExperimentSpec spec = loader.load(yaml);
    assertThat(spec.seed()).isEqualTo(7);
    assertThat(spec.recovery().adaptiveThrottleEnabled()).isFalse();
    assertThat(spec.recovery().riskOrderingEnabled()).isFalse();
    assertThat(spec.recovery().safeFallbackThrottleMiBps()).isEqualTo(12);
    assertThat(spec.recovery().additiveStepMiBps()).isEqualTo(9);
    assertThat(spec.slo().maxViolationTime()).isEqualTo(Duration.ofMinutes(3));
    assertThat(spec.slo().maxViolationAreaMsSeconds()).isEqualTo(1200);
    assertThat(spec.resources().schedule()).singleElement()
        .satisfies(change -> assertThat(change.at()).isEqualTo(Duration.ofMinutes(3)));
  }

  @Test
  void fixedPolicyDoesNotEnableAdaptationImplicitly() {
    assertThat(loader.load(shortYaml().replace("throttlePolicy: AIMD", "throttlePolicy: FIXED"))
        .recovery().adaptiveThrottleEnabled()).isFalse();
  }

  @Test
  void readsUtf8FileAndProducesACompleteIsoDurationSnapshot() throws Exception {
    Path input = directory.resolve("experiment.yaml");
    Files.writeString(input, shortYaml().replace("name: smoke", "name: проверка"));
    ExperimentSpec spec = loader.load(input);
    ObjectMapper json = new ObjectMapper().findAndRegisterModules()
        .disable(SerializationFeature.WRITE_DURATIONS_AS_TIMESTAMPS);
    var snapshot = json.readTree(json.writeValueAsBytes(spec));
    assertThat(snapshot.get("name").asText()).isEqualTo("проверка");
    assertThat(snapshot.get("timeout").asText()).isEqualTo("PT30M");
    assertThat(snapshot.at("/recovery/safeFallbackThrottleMiBps").asLong()).isEqualTo(20);
    assertThat(snapshot.at("/resources/schedule").isArray()).isTrue();
  }

  @ParameterizedTest
  @ValueSource(strings = {"2", "1.0", "\"1\"", "null", "true"})
  void rejectsMissingOrUnsupportedSchemaBeforeBinding(String schema) {
    assertThatThrownBy(() -> loader.load("schemaVersion: " + schema + "\nload: invalid"))
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("schemaVersion");
    assertThatThrownBy(() -> loader.load("name: no-schema"))
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("schemaVersion");
  }

  @ParameterizedTest
  @ValueSource(strings = {
      "schemaVersion: 1\nschemaVersion: 1\n",
      "unknownField: 1\n",
      "seed: null\n"
  })
  void rejectsDuplicatesUnknownFieldsAndExplicitNull(String inserted) {
    String yaml = inserted.startsWith("schemaVersion") ? inserted + shortYaml() : shortYaml() + inserted;
    assertThatThrownBy(() -> loader.load(yaml)).isInstanceOf(IllegalArgumentException.class);
  }

  @ParameterizedTest
  @ValueSource(strings = {"\"20\"", "20.5", "null", "true"})
  void doesNotCoerceInvalidRequiredNumericValues(String value) {
    assertThatThrownBy(() -> loader.load(shortYaml().replace("initialThrottleMiBps: 20",
        "initialThrottleMiBps: " + value))).isInstanceOf(IllegalArgumentException.class);
  }

  @ParameterizedTest
  @ValueSource(strings = {"\"false\"", "0", "null"})
  void doesNotCoerceInvalidBooleanFlags(String value) {
    assertThatThrownBy(() -> loader.load(shortYaml().replace("throttlePolicy: AIMD",
        "throttlePolicy: AIMD\n  adaptiveThrottleEnabled: " + value)))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @ParameterizedTest
  @ValueSource(strings = {"30", "\"30\"", "invalid", "null"})
  void requiresUnitBearingOrIsoDurations(String value) {
    assertThatThrownBy(() -> loader.load(shortYaml().replace("timeout: 30m", "timeout: " + value)))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void rejectsInvalidRecoveryAndMissingRequiredFields() {
    assertThatThrownBy(() -> loader.load(shortYaml().replace("maxThrottleMiBps: 100", "maxThrottleMiBps: 10")))
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("initialThrottleMiBps");
    assertThatThrownBy(() -> loader.load(shortYaml().replace("  initialThrottleMiBps: 20\n", "")))
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("initialThrottleMiBps");
    assertThatThrownBy(() -> loader.load(shortYaml().replace("mode: CONSTANT", "mode: 1")))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> loader.load(shortYaml()
        .replace("profile: normal", "profile: normal\n  schedule: null")))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void rejectsEmptyScalarAndMultipleDocuments() {
    for (String yaml : new String[] {"", "null", "[]", "42", shortYaml() + "\n---\nname: another"}) {
      assertThatThrownBy(() -> loader.load(yaml)).isInstanceOf(IllegalArgumentException.class);
    }
  }

  static String shortYaml() {
    return """
        schemaVersion: 1
        name: smoke
        timeout: 30m
        slo:
          produceP99Ms: 100
          maxErrorRate: 0.01
        load:
          mode: CONSTANT
          messageSizeBytes: 1024
          schedule:
            - duration: 5m
              requestsPerSecond: 1000
        recovery:
          throttlePolicy: AIMD
          initialThrottleMiBps: 20
          minThrottleMiBps: 1
          maxThrottleMiBps: 100
        resources:
          profile: normal
        fault:
          action: STOP_BROKER
          target: broker-2
          at: 2m
        """;
  }
}
