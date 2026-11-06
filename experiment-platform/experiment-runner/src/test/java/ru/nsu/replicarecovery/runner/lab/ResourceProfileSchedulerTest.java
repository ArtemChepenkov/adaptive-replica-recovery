package ru.nsu.replicarecovery.runner.lab;

import static org.assertj.core.api.Assertions.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import ru.nsu.replicarecovery.model.ResourceProfileChange;

class ResourceProfileSchedulerTest {
  @Test void appliesNormalNetworkNormalAtMonotonicOffsetsExactlyOnce() {
    var time = new AtomicLong(42);
    var applied = new ArrayList<String>();
    var scheduler = new ResourceProfileScheduler(List.of(change(0, "normal"), change(2, "network-constrained"),
        change(5, "normal")), applied::add, time::get);
    scheduler.tick();
    assertThat(applied).isEmpty();
    scheduler.start();
    scheduler.tick(); scheduler.tick();
    time.addAndGet(Duration.ofSeconds(2).toNanos());
    scheduler.tick(); scheduler.tick();
    time.addAndGet(Duration.ofSeconds(5).toNanos());
    scheduler.tick(); scheduler.tick();
    assertThat(applied).containsExactly("normal", "network-constrained", "normal");
  }

  @Test void cleanupCancellationAndFailedApplicationPreventFurtherChangesOrImplicitRetry() {
    var time = new AtomicLong();
    var applied = new ArrayList<String>();
    var scheduler = new ResourceProfileScheduler(List.of(change(0, "normal"), change(1, "network-constrained")),
        applied::add, time::get);
    scheduler.start(); scheduler.tick(); scheduler.cancel();
    time.set(Duration.ofSeconds(3).toNanos()); scheduler.tick();
    assertThat(applied).containsExactly("normal");
    var failed = new ResourceProfileScheduler(List.of(change(0, "normal"), change(1, "network-constrained")),
        profile -> { applied.add(profile); throw new IllegalStateException("rejected"); }, time::get);
    failed.start();
    assertThatThrownBy(failed::tick).hasMessage("rejected");
    time.addAndGet(Duration.ofSeconds(3).toNanos()); failed.tick();
    assertThat(applied).containsExactly("normal", "normal");
  }

  @Test void rejectsInvalidOrderOverflowAndUnvalidatedProfileNamesBeforeStart() {
    assertThatThrownBy(() -> new ResourceProfileScheduler(List.of(change(2, "normal"), change(1, "normal")),
        profile -> { }, () -> 0L)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new ResourceProfileScheduler(List.of(change(0, "../normal")),
        profile -> { }, () -> 0L)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new ResourceProfileScheduler(List.of(new ResourceProfileChange(Duration.ofDays(Long.MAX_VALUE / 86400), "normal")),
        profile -> { }, () -> 0L)).isInstanceOf(IllegalArgumentException.class);
  }
  private static ResourceProfileChange change(int seconds, String profile) {
    return new ResourceProfileChange(Duration.ofSeconds(seconds), profile);
  }
}
