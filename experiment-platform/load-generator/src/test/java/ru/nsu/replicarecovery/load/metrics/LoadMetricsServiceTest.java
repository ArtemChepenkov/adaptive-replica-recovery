package ru.nsu.replicarecovery.load.metrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import ru.nsu.replicarecovery.load.core.LoadCommand;
import ru.nsu.replicarecovery.load.core.MessagePublisher;
import ru.nsu.replicarecovery.load.storage.RawRequestLog;
import ru.nsu.replicarecovery.model.ControlSettingsSnapshot;
import ru.nsu.replicarecovery.model.LoadMode;
import ru.nsu.replicarecovery.model.LoadSegment;
import ru.nsu.replicarecovery.model.LoadSpec;
import ru.nsu.replicarecovery.model.RecoverySpec;
import ru.nsu.replicarecovery.model.SloSpec;

class LoadMetricsServiceTest {
  @TempDir
  Path directory;

  private final Clock clock = Clock.fixed(Instant.parse("2026-10-27T10:01:00Z"), ZoneOffset.UTC);

  @Test
  void publisherInitializationDoesNotAdvanceTheReportedLoadSchedule() {
    Instant acceptedAt = Instant.parse("2026-10-27T10:01:00Z");
    AtomicReference<Instant> now = new AtomicReference<>(acceptedAt);
    Clock controlled = mock(Clock.class);
    when(controlled.instant()).thenAnswer(invocation -> now.get());
    try (LoadMetricsService service = new LoadMetricsService(() -> {
      now.set(acceptedAt.plusSeconds(40));
      return new Publisher();
    }, directory, controlled, (path, failure) -> mock(RawRequestLog.class))) {
      var started = service.start(command(), settings());
      assertThat(started.startedAt()).isEqualTo(acceptedAt.plusSeconds(40));
      assertThat(service.current().offeredRps()).isEqualTo(1);
    }
  }

  @Test
  void recordFailureStopsTheEngineAndBlocksRestartUntilLogCleanupFinishes() throws Exception {
    RawRequestLog raw = mock(RawRequestLog.class);
    Publisher publisher = new Publisher(true);
    AtomicReference<Consumer<RuntimeException>> failureListener = new AtomicReference<>();
    UncheckedIOException failure = ioFailure("disk full");
    doAnswer(invocation -> {
      failureListener.get().accept(failure);
      throw failure;
    }).when(raw).record(any(RequestObservation.class));
    CountDownLatch closing = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    doAnswer(invocation -> {
      closing.countDown();
      if (!release.await(3, TimeUnit.SECONDS)) {
        throw new IllegalStateException("test did not release log close");
      }
      return null;
    }).when(raw).close();

    try (LoadMetricsService service = new LoadMetricsService(() -> publisher, directory, clock,
        (path, listener) -> {
          failureListener.set(listener);
          return raw;
        })) {
      service.start(command(), settings());
      assertThat(publisher.sent.await(3, TimeUnit.SECONDS)).isTrue();
      CompletableFuture<Void> callback = CompletableFuture.runAsync(publisher::completePending);
      try {
        assertThat(closing.await(3, TimeUnit.SECONDS)).isTrue();
        assertThat(service.status().state()).isEqualTo("FAILED");
        assertThat(service.status().error()).contains("disk full");
        assertThat(service.status().cleanupComplete()).isFalse();
        assertThat(service.completion()).isNotDone();
        assertThatThrownBy(() -> service.start(command(), settings()))
            .isInstanceOf(LoadMetricsService.RunConflictException.class);
      } finally {
        release.countDown();
      }
      LoadMetricsService.LoadStatus finished = service.completion().get(3, TimeUnit.SECONDS);
      callback.get(3, TimeUnit.SECONDS);
      assertThat(finished.state()).isEqualTo("FAILED");
      assertThat(finished.cleanupComplete()).isTrue();
      assertThat(service.status().outstandingRequests()).isZero();
      assertThat(publisher.sends.get()).isEqualTo(1);
      verify(raw, times(1)).record(any(RequestObservation.class));
      verify(raw, times(1)).close();
      assertThat(publisher.closed.await(3, TimeUnit.SECONDS)).isTrue();
    }
  }

  @Test
  void periodicFlushFailureFailsTheRunEvenWithoutAnotherSend() throws Exception {
    RawRequestLog raw = mock(RawRequestLog.class);
    Publisher publisher = new Publisher();
    AtomicReference<Consumer<RuntimeException>> failureListener = new AtomicReference<>();

    try (LoadMetricsService service = new LoadMetricsService(() -> publisher, directory, clock,
        (path, listener) -> {
          failureListener.set(listener);
          return raw;
        })) {
      service.start(command(), settings());
      assertThat(publisher.sent.await(3, TimeUnit.SECONDS)).isTrue();
      failureListener.get().accept(ioFailure("periodic flush failed"));

      LoadMetricsService.LoadStatus finished = service.completion().get(3, TimeUnit.SECONDS);
      assertThat(finished.state()).isEqualTo("FAILED");
      assertThat(finished.error()).contains("periodic flush failed");
      assertThat(finished.cleanupComplete()).isTrue();
      assertThat(finished.requestCount()).isEqualTo(1);
      assertThat(finished.errorCount()).isZero();
      assertThat(publisher.sends.get()).isEqualTo(1);
      verify(raw, times(1)).close();
    }
  }

  @Test
  void logCreationFailureNeverStartsAPublisherAndCompletesAsFailed() throws Exception {
    AtomicInteger createdPublishers = new AtomicInteger();
    UncheckedIOException failure = ioFailure("cannot create log");
    try (LoadMetricsService service = new LoadMetricsService(() -> {
      createdPublishers.incrementAndGet();
      return new Publisher();
    }, directory, clock, (path, listener) -> {
      listener.accept(failure);
      throw failure;
    })) {
      assertThatThrownBy(() -> service.start(command(), settings())).isSameAs(failure);
      LoadMetricsService.LoadStatus finished = service.completion().get(3, TimeUnit.SECONDS);

      assertThat(finished.state()).isEqualTo("FAILED");
      assertThat(finished.cleanupComplete()).isTrue();
      assertThat(finished.error()).contains("cannot create log");
      assertThat(createdPublishers).hasValue(0);
      assertThat(service.status().requestCount()).isZero();
    }
  }

  @Test
  void logCloseFailureCannotBecomeSuccessfulStop() throws Exception {
    RawRequestLog raw = mock(RawRequestLog.class);
    Publisher publisher = new Publisher();
    AtomicReference<Consumer<RuntimeException>> failureListener = new AtomicReference<>();
    UncheckedIOException failure = ioFailure("close flush failed");
    doAnswer(invocation -> {
      failureListener.get().accept(failure);
      throw failure;
    }).when(raw).close();

    try (LoadMetricsService service = new LoadMetricsService(() -> publisher, directory, clock,
        (path, listener) -> {
          failureListener.set(listener);
          return raw;
        })) {
      service.start(command(), settings());
      assertThat(publisher.sent.await(3, TimeUnit.SECONDS)).isTrue();
      service.stop();

      LoadMetricsService.LoadStatus finished = service.completion().get(3, TimeUnit.SECONDS);
      assertThat(finished.state()).isEqualTo("FAILED");
      assertThat(finished.error()).contains("close flush failed");
      assertThat(finished.cleanupComplete()).isTrue();
      verify(raw, times(1)).close();
      assertThat(publisher.closed.await(3, TimeUnit.SECONDS)).isTrue();
    }
  }

  private static UncheckedIOException ioFailure(String message) {
    return new UncheckedIOException(new IOException(message));
  }

  private static LoadCommand command() {
    return new LoadCommand(new LoadSpec(LoadMode.CONSTANT, 1024,
        List.of(new LoadSegment(Duration.ofSeconds(30), 1))), 42);
  }

  private static ControlSettingsSnapshot settings() {
    return new ControlSettingsSnapshot(new SloSpec(100, 0.01, Duration.ofMinutes(2), 1000),
        new RecoverySpec("AIMD", true, true, 20, 1, 100, 5, 20, 0.7,
            Duration.ofSeconds(10), Duration.ofSeconds(60), Duration.ofSeconds(30),
            10, 0.85, 2, 100, Duration.ofSeconds(20), 5, 1, 3, 3, 3));
  }

  private static final class Publisher implements MessagePublisher {
    final AtomicInteger sends = new AtomicInteger();
    final CountDownLatch sent = new CountDownLatch(1);
    final CountDownLatch closed = new CountDownLatch(1);
    final AtomicReference<Consumer<Exception>> pending = new AtomicReference<>();
    final boolean deferCallback;

    Publisher() {
      this(false);
    }

    Publisher(boolean deferCallback) {
      this.deferCallback = deferCallback;
    }

    @Override
    public void send(long seed, long sequence, byte[] payload, Consumer<Exception> callback) {
      sends.incrementAndGet();
      if (deferCallback) {
        pending.set(callback);
      } else {
        callback.accept(null);
      }
      sent.countDown();
    }

    void completePending() {
      pending.getAndSet(null).accept(null);
    }

    @Override
    public void close(Duration timeout) {
      closed.countDown();
    }
  }
}
