package ru.nsu.replicarecovery.runner.metrics;

import static org.assertj.core.api.Assertions.*;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class DockerStatsSourceTest {
  @Test void readsOnlyExpectedProjectContainersAndDerivesDiskRateFromTwoSamples() {
    MutableClock clock = new MutableClock();
    List<List<String>> commands = new ArrayList<>();
    var source = new DockerStatsSource("replica-recovery", Set.of(1), clock, arguments -> {
      commands.add(arguments);
      if (arguments.get(1).equals("ps")) return "abcdef123456 broker-1\n";
      return "{\"ID\":\"abcdef123456\",\"CPUPerc\":\"12.5%\",\"MemUsage\":\"1MiB / 1GiB\",\"BlockIO\":\"0B / "
          + (clock.instant().getEpochSecond() == 0 ? "1kB" : "2kB") + "\"}\n";
    });
    var first = source.get();
    assertThat(first.cpuPercent()).containsEntry(1, 12.5);
    assertThat(first.memoryBytes()).containsEntry(1, 1048576.0);
    assertThat(first.diskWriteBytesPerSecond()).isEmpty();
    clock.now = Instant.ofEpochSecond(5);
    assertThat(source.get().diskWriteBytesPerSecond()).containsEntry(1, 200.0);
    assertThat(commands.get(0)).contains("--filter", "label=com.docker.compose.project=replica-recovery");
    assertThat(commands.get(1)).contains("stats", "--no-stream", "abcdef123456");
    assertThat(commands.stream().flatMap(List::stream)).doesNotContain("exec", "update", "stop", "kill");
  }

  @Test void missingBrokerAndCounterResetAreUnknownNotZero() {
    MutableClock clock = new MutableClock();
    var source = new DockerStatsSource("replica-recovery", Set.of(1, 2), clock, arguments ->
        arguments.get(1).equals("ps") ? "abcdef123456 broker-1\n"
            : "{\"ID\":\"abcdef123456\",\"CPUPerc\":\"1%\",\"MemUsage\":\"1MB / 2GB\",\"BlockIO\":\"0B / "
                + (clock.instant().getEpochSecond() == 0 ? "2kB" : "1kB") + "\"}\n");
    source.get(); clock.now = Instant.ofEpochSecond(5);
    var next = source.get();
    assertThat(next.cpuPercent()).doesNotContainKey(2);
    assertThat(next.diskWriteBytesPerSecond()).isEmpty();
    assertThat(next.errors()).isNotEmpty();
  }

  @Test void unsafeProjectNamesAndMalformedStatsAreRejected() {
    assertThatThrownBy(() -> new DockerStatsSource("project;rm", Set.of(1), Clock.systemUTC()))
        .isInstanceOf(IllegalArgumentException.class);
    var source = new DockerStatsSource("safe", Set.of(1), Clock.systemUTC(), arguments ->
        arguments.get(1).equals("ps") ? "abcdef123456 broker-1" : "not json");
    assertThat(source.get().cpuPercent()).isEmpty();
  }

  private static final class MutableClock extends Clock {
    Instant now = Instant.EPOCH;
    public ZoneId getZone() { return ZoneOffset.UTC; }
    public Clock withZone(ZoneId zone) { return this; }
    public Instant instant() { return now; }
  }
}
