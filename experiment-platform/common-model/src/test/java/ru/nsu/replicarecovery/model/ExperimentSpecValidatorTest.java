package ru.nsu.replicarecovery.model;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;

class ExperimentSpecValidatorTest {
  private final ExperimentSpecValidator validator = new ExperimentSpecValidator();

  @Test
  void acceptsValidSpec() {
    assertThatCode(() -> validator.validate(Fixtures.validSpec())).doesNotThrowAnyException();
  }

  @Test
  void rejectsUnknownSchemaVersion() {
    ExperimentSpec spec = Fixtures.validSpecWithSchemaVersion(2);

    assertThatThrownBy(() -> validator.validate(spec))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Unsupported schemaVersion: 2");
  }

  @Test
  void rejectsThrottleOutsideBounds() {
    ExperimentSpec spec = Fixtures.validSpecWithInitialThrottleMiBps(101);

    assertThatThrownBy(() -> validator.validate(spec))
        .hasMessage(
            "initialThrottleMiBps must be between minThrottleMiBps and maxThrottleMiBps");
  }

  @Test
  void rejectsBlankName() {
    ExperimentSpec valid = Fixtures.validSpec();
    ExperimentSpec spec = Fixtures.spec(
        1, " ", valid.timeout(), valid.slo(), valid.load(), valid.recovery(),
        valid.resources(), valid.fault());

    assertThatThrownBy(() -> validator.validate(spec)).hasMessage("name must not be blank");
  }

  @Test
  void rejectsInvalidLoadSegment() {
    ExperimentSpec valid = Fixtures.validSpec();
    LoadSpec load = new LoadSpec(
        LoadMode.CONSTANT, 0, List.of(new LoadSegment(Duration.ZERO, 0)));
    ExperimentSpec spec = Fixtures.spec(
        1, valid.name(), valid.timeout(), valid.slo(), load, valid.recovery(),
        valid.resources(), valid.fault());

    assertThatThrownBy(() -> validator.validate(spec))
        .hasMessage("messageSizeBytes must be positive");
  }

  @Test
  void rejectsInvalidSlo() {
    ExperimentSpec valid = Fixtures.validSpec();
    SloSpec slo = new SloSpec(0, 1.1, Duration.ZERO, -1);
    ExperimentSpec spec = Fixtures.spec(
        1, valid.name(), valid.timeout(), slo, valid.load(), valid.recovery(),
        valid.resources(), valid.fault());

    assertThatThrownBy(() -> validator.validate(spec))
        .hasMessage("produceP99Ms must be positive");
  }

  @Test
  void rejectsUnorderedResourceSchedule() {
    ExperimentSpec valid = Fixtures.validSpec();
    ResourceSpec resources = new ResourceSpec(
        "normal",
        List.of(
            new ResourceProfileChange(Duration.ofSeconds(20), "limited"),
            new ResourceProfileChange(Duration.ofSeconds(10), "normal")));
    ExperimentSpec spec = Fixtures.spec(
        1, valid.name(), valid.timeout(), valid.slo(), valid.load(), valid.recovery(),
        resources, valid.fault());

    assertThatThrownBy(() -> validator.validate(spec))
        .hasMessage("resource schedule must be ordered by non-negative offset");
  }

  @Test
  void rejectsSafeFallbackOutsideThrottleBounds() {
    ExperimentSpec valid = Fixtures.validSpec();
    RecoverySpec recovery = new RecoverySpec(
        "AIMD", true, true, 20, 1, 100, 5, 20, 0.7,
        Duration.ofSeconds(10), Duration.ofSeconds(60), Duration.ofSeconds(30),
        101, 0.85, 2, 100, Duration.ofSeconds(20), 5, 1, 3, 3, 3);
    ExperimentSpec spec = Fixtures.spec(
        1, valid.name(), valid.timeout(), valid.slo(), valid.load(), recovery,
        valid.resources(), valid.fault());

    assertThatThrownBy(() -> validator.validate(spec))
        .hasMessage("safeFallbackThrottleMiBps must be between minThrottleMiBps and maxThrottleMiBps");
  }
}

