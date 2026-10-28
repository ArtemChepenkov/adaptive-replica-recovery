package ru.nsu.replicarecovery.load.metrics;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import ru.nsu.replicarecovery.model.LoadMetricsSnapshot;

class SlidingLatencyWindowTest {
  @Test
  void calculatesWindowSnapshot() {
    Instant end = Instant.parse("2026-10-27T10:01:00Z");
    SlidingLatencyWindow window = new SlidingLatencyWindow(Duration.ofSeconds(60));
    for (int ms = 1; ms <= 100; ms++) {
      window.record(new RequestObservation(end.minusSeconds(1), ms, ms * 1000L,
          ms != 100, ms == 100 ? "TimeoutException" : null, 1024));
    }
    LoadMetricsSnapshot snapshot = window.snapshot(end, 100, 4);

    assertThat(snapshot.p99Ms()).isBetween(98.0, 100.0);
    assertThat(snapshot.errorRate()).isEqualTo(0.01);
    assertThat(snapshot.requestCount()).isEqualTo(100);
    assertThat(snapshot.errorCount()).isEqualTo(1);
    assertThat(snapshot.bytesSent()).isEqualTo(99 * 1024);
    assertThat(snapshot.achievedRps()).isEqualTo(99.0 / 60);
    assertThat(snapshot.offeredRps()).isEqualTo(100);
    assertThat(snapshot.outstandingRequests()).isEqualTo(4);
  }

  @Test
  void filtersPartialBoundarySecondsExactlyAsTheMillisecondCsvDoes() {
    Instant end = Instant.parse("2026-10-27T10:01:00.500Z");
    Instant start = end.minusSeconds(60);
    SlidingLatencyWindow window = new SlidingLatencyWindow(Duration.ofSeconds(60));
    long sequence = 0;
    for (Instant timestamp : new Instant[] {
        start.minusMillis(1), start, start.plusMillis(1), end.minusMillis(1), end, end.plusMillis(1)}) {
      window.record(new RequestObservation(timestamp, sequence++, 1000, true, null, 10));
    }

    assertThat(window.snapshot(end, 100, 0).requestCount()).isEqualTo(4);
    assertThat(window.snapshot(end, 100, 0).windowStart()).isEqualTo(start);
    assertThat(window.snapshot(end, 100, 0).windowEnd()).isEqualTo(end);
  }

  @Test
  void expiresIdleWindowsAndDistinguishesNoSuccessesFromZeroLatency() {
    Instant end = Instant.parse("2026-10-27T10:01:00Z");
    SlidingLatencyWindow window = new SlidingLatencyWindow(Duration.ofSeconds(60));
    window.record(new RequestObservation(end, 0, 5000, false, "Error", 100));
    assertThat(window.snapshot(end, 100, 0).p99Ms()).isNull();
    assertThat(window.snapshot(end, 100, 0).errorRate()).isEqualTo(1);
    LoadMetricsSnapshot empty = window.snapshot(end.plusSeconds(61), 100, 0);
    assertThat(empty.requestCount()).isZero();
    assertThat(empty.p50Ms()).isNull();
    assertThat(empty.p95Ms()).isNull();
    assertThat(empty.p99Ms()).isNull();
    assertThat(empty.errorRate()).isZero();
  }

  @Test
  void acceptsOutOfOrderCompletionsAndReusesOnlyTheRequestedWindow() {
    Instant end = Instant.parse("2026-10-27T10:01:00Z");
    SlidingLatencyWindow window = new SlidingLatencyWindow(Duration.ofSeconds(60));
    window.record(new RequestObservation(end, 1, 1000, true, null, 10));
    window.record(new RequestObservation(end.minusSeconds(30), 0, 2000, true, null, 10));
    assertThat(window.snapshot(end, 10, 0).requestCount()).isEqualTo(2);
    assertThat(window.snapshot(end.plusSeconds(31), 10, 0).requestCount()).isEqualTo(1);
  }
}
