package ru.nsu.replicarecovery.load.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import ru.nsu.replicarecovery.model.LoadMode;
import ru.nsu.replicarecovery.model.LoadSegment;
import ru.nsu.replicarecovery.model.LoadSpec;

class RateScheduleTest {
  @Test
  void switchesRateAtStepBoundary() {
    RateSchedule schedule = RateSchedule.of(List.of(
        new LoadSegment(Duration.ofSeconds(30), 100),
        new LoadSegment(Duration.ofSeconds(60), 250)));

    assertThat(schedule.targetRps(Duration.ZERO)).isEqualTo(100);
    assertThat(schedule.targetRps(Duration.ofSeconds(29))).isEqualTo(100);
    assertThat(schedule.targetRps(Duration.ofSeconds(30))).isEqualTo(250);
    assertThat(schedule.isFinished(Duration.ofSeconds(89))).isFalse();
    assertThat(schedule.isFinished(Duration.ofSeconds(90))).isTrue();
    assertThat(schedule.targetRps(Duration.ofSeconds(90))).isZero();
  }

  @ParameterizedTest
  @EnumSource(LoadMode.class)
  void consumesAlreadyCompiledSegmentsForEveryMode(LoadMode mode) {
    List<LoadSegment> segments = mode == LoadMode.CONSTANT
        ? List.of(new LoadSegment(Duration.ofSeconds(3), 100))
        : List.of(new LoadSegment(Duration.ofSeconds(1), 100),
            new LoadSegment(Duration.ofSeconds(1), 500),
            new LoadSegment(Duration.ofSeconds(1), 100));
    RateSchedule schedule = RateSchedule.of(new LoadSpec(mode, 1024, segments).schedule());

    assertThat(schedule.targetRps(Duration.ofMillis(999))).isEqualTo(100);
    assertThat(schedule.targetRps(Duration.ofSeconds(1)))
        .isEqualTo(mode == LoadMode.CONSTANT ? 100 : 500);
    assertThat(schedule.targetRps(Duration.ofSeconds(2))).isEqualTo(100);
    assertThat(schedule.isFinished(Duration.ofSeconds(3))).isTrue();
  }

  @Test
  void retainsSubsecondBoundaries() {
    RateSchedule schedule = RateSchedule.of(List.of(
        new LoadSegment(Duration.ofMillis(250), 3),
        new LoadSegment(Duration.ofMillis(250), 7)));

    assertThat(schedule.targetRps(Duration.ofNanos(249_999_999))).isEqualTo(3);
    assertThat(schedule.targetRps(Duration.ofMillis(250))).isEqualTo(7);
    assertThat(schedule.isFinished(Duration.ofMillis(500))).isTrue();
  }

  @Test
  void splittingSegmentsDoesNotInflateOfferedLoad() {
    RateSchedule split = RateSchedule.of(java.util.stream.IntStream.range(0, 10)
        .mapToObj(index -> new LoadSegment(Duration.ofMillis(100), 1)).toList());
    RateSchedule merged = RateSchedule.of(List.of(new LoadSegment(Duration.ofSeconds(1), 1)));

    assertThat(split.deadlineNanos(0)).isEqualTo(merged.deadlineNanos(0));
    assertThat(split.deadlineNanos(1)).isEqualTo(merged.deadlineNanos(1)).isEqualTo(1_000_000_000L);
  }

  @Test
  void carriesFractionalDemandAcrossRateChanges() {
    RateSchedule schedule = RateSchedule.of(List.of(
        new LoadSegment(Duration.ofMillis(100), 1),
        new LoadSegment(Duration.ofMillis(900), 2)));

    assertThat(schedule.deadlineNanos(0)).isZero();
    assertThat(schedule.deadlineNanos(1)).isEqualTo(550_000_000L);
    assertThat(schedule.deadlineNanos(2)).isEqualTo(1_000_000_000L);
  }

  @Test
  void rejectsInvalidSegmentsAndNegativeTime() {
    assertThatIllegalArgumentException().isThrownBy(() -> RateSchedule.of(List.of()));
    assertThatIllegalArgumentException().isThrownBy(() -> RateSchedule.of(
        List.of(new LoadSegment(Duration.ZERO, 100))));
    assertThatIllegalArgumentException().isThrownBy(() -> RateSchedule.of(
        List.of(new LoadSegment(Duration.ofSeconds(1), 0))));
    RateSchedule schedule = RateSchedule.of(
        List.of(new LoadSegment(Duration.ofSeconds(1), 100)));
    assertThatIllegalArgumentException()
        .isThrownBy(() -> schedule.targetRps(Duration.ofNanos(-1)));
  }
}
