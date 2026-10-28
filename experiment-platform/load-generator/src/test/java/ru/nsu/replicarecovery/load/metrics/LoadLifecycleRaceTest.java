package ru.nsu.replicarecovery.load.metrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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

class LoadLifecycleRaceTest {
  @TempDir Path directory;

  @Test
  void stopReturnsWhileBlockedLogCloseKeepsRestartForbidden() throws Exception {
    RawRequestLog raw = mock(RawRequestLog.class);
    CountDownLatch closing = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    doAnswer(invocation -> {
      closing.countDown();
      if (!release.await(10, TimeUnit.SECONDS)) {
        throw new IllegalStateException("test did not release log close");
      }
      return null;
    }).when(raw).close();
    CountDownLatch sent = new CountDownLatch(1);
    try (LoadMetricsService service = new LoadMetricsService(() -> publisher(sent), directory,
        Clock.systemUTC(), (path, failure) -> raw, Duration.ofMillis(50))) {
      service.start(command(Duration.ofSeconds(30)), settings());
      assertThat(sent.await(3, TimeUnit.SECONDS)).isTrue();
      CompletableFuture<LoadMetricsService.LoadStatus> stopping =
          CompletableFuture.supplyAsync(service::stop);
      try {
        assertThat(closing.await(3, TimeUnit.SECONDS)).isTrue();
        LoadMetricsService.LoadStatus returned;
        try {
          returned = stopping.get(3, TimeUnit.SECONDS);
        } catch (TimeoutException exception) {
          System.err.println("THREAD DUMP: stop has not returned while raw.close is blocked");
          for (var info : ManagementFactory.getThreadMXBean().dumpAllThreads(true, true)) {
            System.err.println("THREAD " + info.getThreadName() + " state=" + info.getThreadState()
                + " lock=" + info.getLockName() + " owner=" + info.getLockOwnerName());
            for (var frame : info.getStackTrace()) {
              System.err.println("  at " + frame);
            }
          }
          throw exception;
        }
        assertThat(returned.cleanupComplete()).isFalse();
        assertThat(returned.state()).isEqualTo("STOPPING");
        assertThat(service.completion()).isNotDone();
        assertThatThrownBy(() -> service.start(command(Duration.ofSeconds(30)), settings()))
            .isInstanceOf(LoadMetricsService.RunConflictException.class);
      } finally {
        release.countDown();
      }
      var completed = service.completion().get(3, TimeUnit.SECONDS);
      assertThat(completed.state()).isEqualTo("STOPPED");
      assertThat(completed.cleanupComplete()).isTrue();
    }
  }

  @Test
  void lateFailureNotificationCannotRewriteAnAlreadyCompletedStatus() throws Exception {
    RawRequestLog raw = mock(RawRequestLog.class);
    AtomicReference<Consumer<RuntimeException>> listener = new AtomicReference<>();
    try (LoadMetricsService service = new LoadMetricsService(() -> publisher(new CountDownLatch(1)),
        directory, Clock.systemUTC(), (path, failure) -> {
          listener.set(failure);
          return raw;
        })) {
      service.start(command(Duration.ofMillis(5)), settings());
      var completed = service.completion().get(3, TimeUnit.SECONDS);
      assertThat(completed.state()).isEqualTo("COMPLETED");
      listener.get().accept(new UncheckedIOException(new IOException("late stale notification")));
      assertThat(service.status().state()).isEqualTo(completed.state());
      assertThat(service.status().error()).isNull();
      assertThat(service.stop().state()).isEqualTo("COMPLETED");
    }
  }

  @Test
  void concurrentStopsAndNaturalCompletionAlwaysLeaveATerminalState() throws Exception {
    var callers = Executors.newFixedThreadPool(2);
    try (LoadMetricsService service = new LoadMetricsService(
        () -> publisher(new CountDownLatch(1)), directory, Clock.systemUTC(),
        (path, failure) -> mock(RawRequestLog.class))) {
      for (int run = 0; run < 25; run++) {
        service.start(command(Duration.ofMillis(1)), settings());
        CompletableFuture<?> first = CompletableFuture.runAsync(service::stop, callers);
        CompletableFuture<?> second = CompletableFuture.runAsync(service::stop, callers);
        first.get(3, TimeUnit.SECONDS);
        second.get(3, TimeUnit.SECONDS);
        var completed = service.completion().get(3, TimeUnit.SECONDS);
        assertThat(completed.state()).isIn("COMPLETED", "STOPPED");
        assertThat(service.status().state()).isEqualTo(completed.state());
        assertThat(service.status().cleanupComplete()).isTrue();
      }
    } finally {
      callers.shutdownNow();
    }
  }

  private static MessagePublisher publisher(CountDownLatch sent) {
    return new MessagePublisher() {
      @Override
      public void send(long seed, long sequence, byte[] payload, Consumer<Exception> callback) {
        callback.accept(null);
        sent.countDown();
      }

      @Override
      public void close(Duration timeout) { }
    };
  }

  private static LoadCommand command(Duration duration) {
    return new LoadCommand(new LoadSpec(LoadMode.CONSTANT, 1024,
        List.of(new LoadSegment(duration, 1))), 42);
  }

  private static ControlSettingsSnapshot settings() {
    return new ControlSettingsSnapshot(new SloSpec(100, 0.01, Duration.ofMinutes(2), 1000),
        new RecoverySpec("AIMD", true, true, 20, 1, 100, 5, 20, 0.7,
            Duration.ofSeconds(10), Duration.ofSeconds(60), Duration.ofSeconds(30),
            10, 0.85, 2, 100, Duration.ofSeconds(20), 5, 1, 3, 3, 3));
  }
}
