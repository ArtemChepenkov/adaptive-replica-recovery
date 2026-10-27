package ru.nsu.replicarecovery.load.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import ru.nsu.replicarecovery.load.metrics.RequestObservation;
import ru.nsu.replicarecovery.model.LoadMode;
import ru.nsu.replicarecovery.model.LoadSegment;
import ru.nsu.replicarecovery.model.LoadSpec;

class OpenLoopLoadEngineTest {
  @Test
  void submitsOneHundredRequestsDespiteTwoHundredMillisecondResponses() {
    Fixture fixture = new Fixture();
    fixture.publisher.delay = Duration.ofMillis(200);
    fixture.engine.start(command(Duration.ofSeconds(2), 100, 42));

    fixture.time.advanceTo(999_999_999L);

    assertThat(fixture.publisher.submissions).hasSize(100);
    assertThat(fixture.engine.outstandingRequests()).isEqualTo(20);
    assertThat(fixture.observations).hasSize(80);
    assertThat(fixture.observations).allSatisfy(observation ->
        assertThat(observation.latencyMicros()).isEqualTo(200_000));
    fixture.time.advanceTo(1_200_000_000L);
    fixture.publisher.completeAll();
    fixture.engine.stop();
  }

  @Test
  void stopsSubmittingAndRecordsUnansweredRequestsAtDrainTimeout() throws Exception {
    Fixture fixture = new Fixture();
    fixture.engine.start(command(Duration.ofSeconds(10), 100, 42));
    fixture.time.advanceTo(90_000_000L);

    long before = System.nanoTime();
    try {
      fixture.engine.stop();
    } catch (IllegalStateException exception) {
      assertThat(exception).hasMessage("load did not finish before stop deadline");
    }
    fixture.engine.completion().get(1, TimeUnit.SECONDS);

    assertThat(Duration.ofNanos(System.nanoTime() - before)).isLessThan(Duration.ofSeconds(1));
    assertThat(fixture.publisher.submissions).hasSize(10);
    assertThat(fixture.engine.outstandingRequests()).isZero();
    assertThat(fixture.publisher.closes).isEqualTo(1);
    assertThat(fixture.observations).hasSize(10).allSatisfy(observation -> {
      assertThat(observation.success()).isFalse();
      assertThat(observation.errorClass()).isEqualTo("java.util.concurrent.TimeoutException");
    });
    fixture.time.advanceTo(2_000_000_000L);
    fixture.publisher.completeAll();
    fixture.engine.stop();
    assertThat(fixture.observations).hasSize(10);
    assertThat(fixture.publisher.closes).isEqualTo(1);
  }

  @Test
  void reusesSeededPayloadAndRestartsSequenceOnNextRun() {
    Fixture fixture = new Fixture();
    fixture.publisher.delay = Duration.ZERO;
    LoadCommand command = command(Duration.ofSeconds(1), 3, 42);
    fixture.engine.start(command);
    assertThatIllegalStateException().isThrownBy(() -> fixture.engine.start(command));
    fixture.time.advanceTo(900_000_000L);
    fixture.engine.stop();
    List<Submission> first = List.copyOf(fixture.publisher.submissions);
    fixture.publisher.submissions.clear();
    fixture.engine.start(command);
    fixture.time.advanceTo(1_800_000_000L);
    fixture.engine.stop();

    assertThat(first).hasSize(3);
    assertThat(fixture.publisher.submissions).hasSize(3);
    assertThat(first.get(0).payload()).hasSize(128);
    assertThat(first.get(1).payload()).isSameAs(first.get(0).payload());
    assertThat(fixture.publisher.submissions.get(0).payload()).isEqualTo(first.get(0).payload());
    assertThat(fixture.publisher.submissions).extracting(Submission::sequence)
        .containsExactly(0L, 1L, 2L);
  }

  @Test
  void countsSynchronousSendErrorsAndDuplicateCallbacksOnce() {
    Fixture fixture = new Fixture();
    fixture.publisher.failure = new IllegalStateException("send failed");
    fixture.engine.start(command(Duration.ofSeconds(1), 1, 42));
    fixture.time.advanceTo(0);
    fixture.engine.stop();
    assertThat(fixture.observations).singleElement().satisfies(observation -> {
      assertThat(observation.success()).isFalse();
      assertThat(observation.errorClass()).isEqualTo("java.lang.IllegalStateException");
      assertThat(observation.bytes()).isEqualTo(128);
    });
    fixture.publisher.completeAll();
    assertThat(fixture.observations).hasSize(1);
  }

  @Test
  void waitsForOutstandingSuccessBeforeClosingPublisher() throws Exception {
    Fixture fixture = new Fixture();
    fixture.engine.start(command(Duration.ofSeconds(2), 1, 42));
    fixture.time.advanceTo(0);
    CompletableFuture<Void> stopping = CompletableFuture.runAsync(fixture.engine::stop);
    fixture.publisher.completeAll();
    stopping.get(1, TimeUnit.SECONDS);

    assertThat(fixture.observations).singleElement()
        .satisfies(observation -> assertThat(observation.success()).isTrue());
    assertThat(fixture.publisher.closes).isEqualTo(1);
  }

  @Test
  void cannotFinishSuccessfullyOrRestartWhileObservationIsStillBeingWritten() throws Exception {
    VirtualTime time = new VirtualTime();
    FakePublisher publisher = new FakePublisher(time);
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    OpenLoopLoadEngine engine = new OpenLoopLoadEngine(() -> publisher, observation -> {
      entered.countDown();
      try {
        if (!release.await(2, TimeUnit.SECONDS)) {
          throw new IllegalStateException("test observer was not released");
        }
      } catch (InterruptedException exception) {
        throw new IllegalStateException(exception);
      }
    }, time.scheduler, () -> time.now, time.clock, Duration.ofMillis(30));
    LoadCommand command = command(Duration.ofSeconds(2), 1, 42);
    engine.start(command);
    time.advanceTo(0);
    CompletableFuture<Void> callback = CompletableFuture.runAsync(publisher::completeAll);
    try {
      assertThat(entered.await(1, TimeUnit.SECONDS)).isTrue();
      assertThatIllegalStateException().isThrownBy(engine::stop);
      assertThat(engine.completion()).isNotDone();
      assertThatIllegalStateException().isThrownBy(() -> engine.start(command));
    } finally {
      release.countDown();
    }
    callback.get(1, TimeUnit.SECONDS);
    assertThatThrownBy(() -> engine.completion().get(1, TimeUnit.SECONDS))
        .isInstanceOf(ExecutionException.class).hasCauseInstanceOf(IllegalStateException.class);
  }

  @Test
  void boundsStopEvenWhenPublisherCloseBlocks() throws Exception {
    VirtualTime time = new VirtualTime();
    MessagePublisher publisher = mock(MessagePublisher.class);
    CountDownLatch closing = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    doAnswer(invocation -> {
      closing.countDown();
      if (!release.await(2, TimeUnit.SECONDS)) {
        throw new IllegalStateException("test close was not released");
      }
      return null;
    }).when(publisher).close(any(Duration.class));
    OpenLoopLoadEngine engine = new OpenLoopLoadEngine(() -> publisher, observation -> { },
        time.scheduler, () -> time.now, time.clock, Duration.ofMillis(30));
    LoadCommand command = command(Duration.ofSeconds(2), 1, 42);
    engine.start(command);
    long before = System.nanoTime();
    try {
      assertThatIllegalStateException().isThrownBy(engine::stop);
      assertThat(Duration.ofNanos(System.nanoTime() - before)).isLessThan(Duration.ofSeconds(1));
      assertThat(closing.await(1, TimeUnit.SECONDS)).isTrue();
      assertThat(engine.completion()).isNotDone();
      assertThatIllegalStateException().isThrownBy(() -> engine.start(command));
    } finally {
      release.countDown();
    }
    engine.completion().get(1, TimeUnit.SECONDS);
  }

  @Test
  void completesAWholeRunWithTheRealScheduler() throws Exception {
    List<RequestObservation> observations = new CopyOnWriteArrayList<>();
    MessagePublisher publisher = mock(MessagePublisher.class);
    doAnswer(invocation -> {
      Consumer<Exception> callback = invocation.getArgument(3);
      callback.accept(null);
      return null;
    }).when(publisher).send(anyLong(), anyLong(), any(byte[].class), any());

    try (OpenLoopLoadEngine engine = new OpenLoopLoadEngine(() -> publisher, observations::add)) {
      engine.start(command(Duration.ofMillis(100), 100, 42));
      engine.completion().get(3, TimeUnit.SECONDS);
      assertThat(observations).hasSize(10);
      assertThat(observations).allSatisfy(observation -> assertThat(observation.success()).isTrue());
    }
  }

  @Test
  void finishesAutomaticallyAndIncludesSchedulerDelayInLatency() throws Exception {
    Fixture fixture = new Fixture();
    fixture.publisher.delay = Duration.ZERO;
    fixture.engine.start(command(Duration.ofSeconds(1), 1, 42));
    fixture.time.now = 50_000_000L;
    fixture.time.advanceTo(1_000_000_000L);
    fixture.engine.completion().get(1, TimeUnit.SECONDS);

    assertThat(fixture.observations).singleElement().satisfies(observation -> {
      assertThat(observation.latencyMicros()).isEqualTo(50_000);
      assertThat(observation.completedAt()).isEqualTo(Instant.parse("2026-10-27T00:00:00.050Z"));
    });
    assertThat(fixture.publisher.closes).isEqualTo(1);
    assertThat(fixture.engine.isRunning()).isFalse();
  }

  private static LoadCommand command(Duration duration, long rate, long seed) {
    return new LoadCommand(new LoadSpec(LoadMode.CONSTANT, 128,
        List.of(new LoadSegment(duration, rate))), seed);
  }

  private static final class Fixture {
    final VirtualTime time = new VirtualTime();
    final FakePublisher publisher = new FakePublisher(time);
    final List<RequestObservation> observations = new CopyOnWriteArrayList<>();
    final OpenLoopLoadEngine engine = new OpenLoopLoadEngine(() -> publisher, observations::add,
        time.scheduler, () -> time.now, time.clock, Duration.ofMillis(30));
  }

  private record Submission(long seed, long sequence, byte[] payload, Consumer<Exception> callback) {
  }

  private static final class FakePublisher implements MessagePublisher {
    final VirtualTime time;
    final List<Submission> submissions = new ArrayList<>();
    Duration delay;
    RuntimeException failure;
    volatile int closes;

    FakePublisher(VirtualTime time) {
      this.time = time;
    }

    @Override
    public void send(long seed, long sequence, byte[] payload, Consumer<Exception> callback) {
      submissions.add(new Submission(seed, sequence, payload, callback));
      if (failure != null) {
        throw failure;
      }
      if (delay != null) {
        time.scheduler.schedule(() -> callback.accept(null), delay.toNanos(), TimeUnit.NANOSECONDS);
      }
    }

    void completeAll() {
      submissions.forEach(submission -> submission.callback().accept(null));
    }

    @Override
    public void close(Duration timeout) {
      closes++;
    }
  }

  private static final class VirtualTime {
    long now;
    long order;
    final ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
    final Clock clock = mock(Clock.class);
    final PriorityQueue<TimedTask> tasks = new PriorityQueue<>(
        Comparator.comparingLong(TimedTask::at).thenComparingLong(TimedTask::order));

    VirtualTime() {
      when(clock.instant()).thenAnswer(invocation ->
          Instant.parse("2026-10-27T00:00:00Z").plusNanos(now));
      when(clock.getZone()).thenReturn(ZoneOffset.UTC);
      doAnswer(invocation -> {
        Runnable action = invocation.getArgument(0);
        long delay = invocation.getArgument(1);
        TimeUnit unit = invocation.getArgument(2);
        ScheduledFuture<?> future = mock(ScheduledFuture.class);
        tasks.add(new TimedTask(now + unit.toNanos(delay), order++, action, future));
        return future;
      }).when(scheduler).schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
    }

    void advanceTo(long end) {
      while (!tasks.isEmpty() && tasks.peek().at() <= end) {
        TimedTask task = tasks.remove();
        now = Math.max(now, task.at());
        task.action().run();
      }
      now = end;
    }
  }

  private record TimedTask(long at, long order, Runnable action, ScheduledFuture<?> future) {
  }
}
