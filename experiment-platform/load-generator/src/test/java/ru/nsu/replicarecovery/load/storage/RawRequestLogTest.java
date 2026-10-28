package ru.nsu.replicarecovery.load.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import ru.nsu.replicarecovery.load.metrics.RequestObservation;

class RawRequestLogTest {
  @TempDir
  Path temporary;

  @Test
  void writesHeaderUtcMillisecondsAndAllObservationFields() throws Exception {
    Path path = temporary.resolve("run/requests.csv");
    List<RuntimeException> failures = new ArrayList<>();
    try (RawRequestLog log = new RawRequestLog(path, failures::add)) {
      log.record(observation(0, 1500, true, null));
      log.record(observation(1, 2000, false, "java.util.concurrent.TimeoutException"));
      log.flush();
      assertThat(Files.readString(path, StandardCharsets.UTF_8)).isEqualTo(RawRequestLog.HEADER
          + "1793059200123,0,1500,true,,1024\n"
          + "1793059200123,1,2000,false,java.util.concurrent.TimeoutException,1024\n");
    }
    assertThat(failures).isEmpty();
  }

  @Test
  void doesNotOverwriteAnExistingRun() throws Exception {
    Path path = temporary.resolve("requests.csv");
    Files.writeString(path, "previous results");
    List<RuntimeException> failures = new ArrayList<>();

    assertThatThrownBy(() -> new RawRequestLog(path, failures::add))
        .isInstanceOf(UncheckedIOException.class);
    assertThat(Files.readString(path)).isEqualTo("previous results");
    assertThat(failures).hasSize(1);
  }

  @Test
  void flushesEverySecondAndAtCloseWithoutSleeping() {
    StringWriter output = new StringWriter();
    BufferedWriter writer = new BufferedWriter(output);
    ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
    AtomicReference<Runnable> tick = captureTick(scheduler);
    RawRequestLog log = new RawRequestLog(writer, failure -> { }, scheduler);

    log.record(observation(0, 1234, true, null));
    assertThat(output.toString()).isEqualTo(RawRequestLog.HEADER);
    tick.get().run();
    assertThat(output.toString()).endsWith("1793059200123,0,1234,true,,1024\n");
    log.record(observation(1, 5678, true, null));
    log.close();
    log.close();

    assertThat(output.toString()).endsWith("1793059200123,1,5678,true,,1024\n");
    verify(scheduler).scheduleAtFixedRate(any(Runnable.class), eq(1L), eq(1L), eq(TimeUnit.SECONDS));
    assertThatIllegalStateException().isThrownBy(() -> log.record(observation(2, 1, true, null)));
  }

  @Test
  void escapesCsvFields() {
    StringWriter output = new StringWriter();
    try (RawRequestLog log = new RawRequestLog(new BufferedWriter(output), failure -> { },
        mock(ScheduledExecutorService.class))) {
      log.record(observation(0, 12, false, "custom,\"error\"\nnext"));
    }
    assertThat(output.toString()).contains("false,\"custom,\"\"error\"\"\nnext\",1024\n");
  }

  @Test
  void serializesConcurrentCallbacksIntoCompleteRows() {
    StringWriter output = new StringWriter();
    try (RawRequestLog log = new RawRequestLog(new BufferedWriter(output), failure -> { },
        mock(ScheduledExecutorService.class))) {
      CompletableFuture<?>[] writes = java.util.stream.IntStream.range(0, 100)
          .mapToObj(sequence -> CompletableFuture.runAsync(() ->
              log.record(observation(sequence, sequence, true, null))))
          .toArray(CompletableFuture[]::new);
      CompletableFuture.allOf(writes).join();
    }
    List<String> rows = output.toString().lines().skip(1).toList();
    assertThat(rows).hasSize(100);
    assertThat(rows).allSatisfy(row -> assertThat(row.split(",", -1)).hasSize(6));
    assertThat(rows.stream().map(row -> Long.parseLong(row.split(",")[1])).distinct()).hasSize(100);
  }

  @Test
  void reportsWriteFailureOnceRejectsFurtherWritesAndStillClosesWriter() throws Exception {
    FailingWriter writer = new FailingWriter();
    List<RuntimeException> failures = new ArrayList<>();
    RawRequestLog log = new RawRequestLog(writer, failures::add,
        mock(ScheduledExecutorService.class));
    writer.failWrite = true;

    assertThatThrownBy(() -> log.record(observation(0, 1, true, null)))
        .isInstanceOf(UncheckedIOException.class);
    writer.failWrite = false;
    assertThatThrownBy(() -> log.record(observation(1, 1, true, null)))
        .isInstanceOf(UncheckedIOException.class);
    assertThatThrownBy(log::flush).isInstanceOf(UncheckedIOException.class);
    assertThatThrownBy(log::close).isSameAs(failures.get(0));

    assertThat(failures).hasSize(1);
    assertThat(writer.closed).isTrue();
    assertThat(writer.output.toString()).isEqualTo(RawRequestLog.HEADER);
  }

  @Test
  void reportsBackgroundFlushFailureAndAttemptsCloseEvenWhenFlushFails() {
    FailingWriter writer = new FailingWriter();
    List<RuntimeException> failures = new ArrayList<>();
    ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
    AtomicReference<Runnable> tick = captureTick(scheduler);
    RawRequestLog log = new RawRequestLog(writer, failures::add, scheduler);
    writer.failFlush = true;

    tick.get().run();
    tick.get().run();
    assertThat(failures).hasSize(1);
    assertThatThrownBy(log::close).isInstanceOf(UncheckedIOException.class);
    log.close();
    assertThat(writer.closed).isTrue();
    assertThat(failures).hasSize(1);
  }

  @Test
  void closeRetainsAnEarlierFailureWhileItsNotificationIsStillInProgress() throws Exception {
    FailingWriter writer = new FailingWriter();
    CountDownLatch notifying = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    AtomicInteger notifications = new AtomicInteger();
    RawRequestLog log = new RawRequestLog(writer, failure -> {
      notifications.incrementAndGet();
      notifying.countDown();
      try {
        if (!release.await(3, TimeUnit.SECONDS)) {
          throw new IllegalStateException("test did not release failure notification");
        }
      } catch (InterruptedException exception) {
        throw new IllegalStateException(exception);
      }
    }, mock(ScheduledExecutorService.class));
    writer.failFlush = true;
    CompletableFuture<Void> flushing = CompletableFuture.runAsync(() ->
        assertThatThrownBy(log::flush).isInstanceOf(UncheckedIOException.class));
    try {
      assertThat(notifying.await(3, TimeUnit.SECONDS)).isTrue();
      writer.failFlush = false;
      assertThatThrownBy(log::close).isInstanceOf(UncheckedIOException.class)
          .hasMessageContaining("flush failed");
      assertThat(writer.closed).isTrue();
      log.close();
      assertThat(notifications).hasValue(1);
    } finally {
      release.countDown();
    }
    flushing.get(3, TimeUnit.SECONDS);
  }

  @Test
  void closesWriterAndSchedulerWhenHeaderCannotBeWritten() {
    FailingWriter writer = new FailingWriter();
    writer.failWrite = true;
    List<RuntimeException> failures = new ArrayList<>();
    ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);

    assertThatThrownBy(() -> new RawRequestLog(writer, failures::add, scheduler))
        .isInstanceOf(UncheckedIOException.class);
    assertThat(writer.closed).isTrue();
    verify(scheduler).shutdownNow();
    assertThat(failures).hasSize(1);
  }

  private static AtomicReference<Runnable> captureTick(ScheduledExecutorService scheduler) {
    AtomicReference<Runnable> tick = new AtomicReference<>();
    doAnswer(invocation -> {
      tick.set(invocation.getArgument(0));
      return null;
    }).when(scheduler).scheduleAtFixedRate(any(Runnable.class), eq(1L), eq(1L), eq(TimeUnit.SECONDS));
    return tick;
  }

  private static RequestObservation observation(long sequence, long micros, boolean success, String error) {
    return new RequestObservation(Instant.parse("2026-10-27T00:00:00.123456Z"), sequence,
        micros, success, error, 1024);
  }

  private static final class FailingWriter extends BufferedWriter {
    final StringWriter output;
    boolean failWrite;
    boolean failFlush;
    boolean closed;

    FailingWriter() {
      this(new StringWriter());
    }

    private FailingWriter(StringWriter output) {
      super(output);
      this.output = output;
    }

    @Override
    public void write(String value, int offset, int length) throws IOException {
      if (failWrite) {
        throw new IOException("write failed");
      }
      super.write(value, offset, length);
    }

    @Override
    public void flush() throws IOException {
      if (failFlush) {
        throw new IOException("flush failed");
      }
      super.flush();
    }

    @Override
    public void close() throws IOException {
      closed = true;
      super.close();
    }
  }
}
