package ru.nsu.replicarecovery.load.core;

import java.time.Clock;
import java.time.Duration;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import ru.nsu.replicarecovery.load.metrics.RequestObservation;

/** A single scheduling thread issues sends without waiting for their callbacks. */
public final class OpenLoopLoadEngine implements AutoCloseable {
  private final Supplier<MessagePublisher> publishers;
  private final Consumer<RequestObservation> observations;
  private final ScheduledExecutorService scheduler;
  private final ExecutorService finisher = Executors.newSingleThreadExecutor(action -> {
    Thread thread = new Thread(action, "open-loop-load-finish");
    thread.setDaemon(true);
    return thread;
  });
  private final LongSupplier nanoTime;
  private final Clock clock;
  private final Duration drainTimeout;
  private Run active;
  private boolean closed;

  public OpenLoopLoadEngine(
      Supplier<MessagePublisher> publishers, Consumer<RequestObservation> observations) {
    this(publishers, observations, Clock.systemUTC());
  }

  public OpenLoopLoadEngine(Supplier<MessagePublisher> publishers,
      Consumer<RequestObservation> observations, Clock clock) {
    this(publishers, observations, Executors.newSingleThreadScheduledExecutor(action -> {
      Thread thread = new Thread(action, "open-loop-load");
      thread.setDaemon(true);
      return thread;
    }), System::nanoTime, clock, Duration.ofSeconds(10));
  }

  // Virtual scheduling and short drain deadlines keep unit tests independent of wall time.
  OpenLoopLoadEngine(Supplier<MessagePublisher> publishers,
      Consumer<RequestObservation> observations, ScheduledExecutorService scheduler,
      LongSupplier nanoTime, Clock clock, Duration drainTimeout) {
    if (drainTimeout.isNegative() || drainTimeout.compareTo(Duration.ofSeconds(10)) > 0) {
      throw new IllegalArgumentException("drainTimeout must be in [0, 10 seconds]");
    }
    this.publishers = publishers;
    this.observations = observations;
    this.scheduler = scheduler;
    this.nanoTime = nanoTime;
    this.clock = clock;
    this.drainTimeout = drainTimeout;
  }

  public synchronized void start(LoadCommand command) {
    if (closed || (active != null && !active.completion.isDone())) {
      throw new IllegalStateException("engine is closed or a previous run has not finished");
    }
    RateSchedule schedule = RateSchedule.of(command.load().schedule());
    byte[] payload = new byte[command.load().messageSizeBytes()];
    new Random(command.seed()).nextBytes(payload);
    active = new Run(command, schedule, payload, publishers.get(), nanoTime.getAsLong());
    scheduleNext(active);
  }

  public synchronized boolean isRunning() {
    return active != null && !active.completion.isDone();
  }

  public synchronized int outstandingRequests() {
    return active == null ? 0 : active.pending.size();
  }

  public synchronized CompletableFuture<Void> completion() {
    return active == null ? CompletableFuture.completedFuture(null) : active.completion.copy();
  }

  public void stop() {
    long stopStarted = System.nanoTime();
    Run run;
    synchronized (this) {
      run = active;
      if (run == null) {
        return;
      }
      stopScheduling(run);
    }
    requestFinish(run);
    try {
      long remaining = Math.max(0, drainTimeout.toNanos() - (System.nanoTime() - stopStarted));
      run.completion.get(remaining, TimeUnit.NANOSECONDS);
    } catch (TimeoutException exception) {
      IllegalStateException failure = new IllegalStateException("load did not finish before stop deadline", exception);
      run.failure = failure;
      throw failure;
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("interrupted while stopping the load", exception);
    } catch (ExecutionException exception) {
      throw new CompletionException(exception.getCause());
    }
  }

  @Override
  public void close() {
    synchronized (this) {
      if (closed) {
        return;
      }
      closed = true;
    }
    try {
      stop();
    } finally {
      scheduler.shutdownNow();
      finisher.shutdown();
    }
  }

  private void scheduleNext(Run run) {
    long deadline = run.schedule.deadlineNanos(run.sequence);
    long delay = Math.max(0, deadline - (nanoTime.getAsLong() - run.startedAt));
    try {
      run.next = scheduler.schedule(() -> submit(run), delay, TimeUnit.NANOSECONDS);
    } catch (RuntimeException exception) {
      run.failure = exception;
      stopScheduling(run);
      requestFinish(run);
    }
  }

  private synchronized void submit(Run run) {
    if (active != run || !run.accepting) {
      return;
    }
    long deadline = run.schedule.deadlineNanos(run.sequence);
    if (deadline == run.schedule.duration().toNanos()) {
      stopScheduling(run);
      // Draining must not block the scheduling thread or Kafka's callback thread.
      requestFinish(run);
      return;
    }
    long sequence = run.sequence++;
    Pending request = new Pending(deadline);
    run.pending.put(sequence, request);
    try {
      run.publisher.send(run.command.seed(), sequence, run.payload,
          error -> complete(run, sequence, request, error));
    } catch (RuntimeException exception) {
      complete(run, sequence, request, exception);
    }
    if (run.accepting) {
      scheduleNext(run);
    }
  }

  private void complete(Run run, long sequence, Pending request, Exception error) {
    if (!request.completed.compareAndSet(false, true)) {
      return;
    }
    long elapsed = nanoTime.getAsLong() - run.startedAt;
    try {
      observations.accept(new RequestObservation(clock.instant(), sequence,
          Math.max(0, elapsed - request.deadline) / 1_000,
          error == null, error == null ? null : error.getClass().getName(), run.payload.length));
    } catch (RuntimeException exception) {
      synchronized (this) {
        run.failure = exception;
        stopScheduling(run);
      }
      requestFinish(run);
    } finally {
      run.pending.remove(sequence, request);
      synchronized (run.drainMonitor) {
        run.drainMonitor.notifyAll();
      }
      completeRunIfDrained(run);
    }
  }

  // Called with the engine monitor held, so no send can begin after it returns.
  private void stopScheduling(Run run) {
    run.accepting = false;
    if (run.next != null) {
      run.next.cancel(false);
    }
  }

  private void requestFinish(Run run) {
    if (run.completion.isDone() || !run.finishRequested.compareAndSet(false, true)) {
      return;
    }
    // A dedicated executor prevents ForkJoinPool timed waits from executing a
    // blocking close inline via CompletableFuture's helpAsyncBlocker path.
    finisher.execute(() -> finish(run));
  }

  private void finish(Run run) {
    boolean interrupted = false;
    try {
      long started = System.nanoTime();
      synchronized (run.drainMonitor) {
        long remaining = drainTimeout.toNanos();
        while (!run.pending.isEmpty() && remaining > 0) {
          try {
            TimeUnit.NANOSECONDS.timedWait(run.drainMonitor, remaining);
          } catch (InterruptedException exception) {
            interrupted = true;
            break;
          }
          remaining = drainTimeout.toNanos() - (System.nanoTime() - started);
        }
      }
      run.pending.forEach((sequence, request) -> complete(run, sequence, request,
          new TimeoutException("request did not complete before the stop deadline")));
      if (!run.pending.isEmpty()) {
        run.failure = new IllegalStateException("observation processing exceeded the stop deadline");
      }
      run.publisher.close(Duration.ZERO);
    } catch (RuntimeException exception) {
      run.failure = exception;
    } finally {
      run.publisherClosed = true;
      completeRunIfDrained(run);
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }

  private void completeRunIfDrained(Run run) {
    if (run.publisherClosed && run.pending.isEmpty()) {
      if (run.failure == null) {
        run.completion.complete(null);
      } else {
        run.completion.completeExceptionally(run.failure);
      }
    }
  }

  private static final class Pending {
    final long deadline;
    final AtomicBoolean completed = new AtomicBoolean();

    Pending(long deadline) {
      this.deadline = deadline;
    }
  }

  private static final class Run {
    final LoadCommand command;
    final RateSchedule schedule;
    final byte[] payload;
    final MessagePublisher publisher;
    final long startedAt;
    final ConcurrentHashMap<Long, Pending> pending = new ConcurrentHashMap<>();
    final Object drainMonitor = new Object();
    final AtomicBoolean finishRequested = new AtomicBoolean();
    final CompletableFuture<Void> completion = new CompletableFuture<>();
    volatile RuntimeException failure;
    volatile boolean publisherClosed;
    ScheduledFuture<?> next;
    boolean accepting = true;
    long sequence;

    Run(LoadCommand command, RateSchedule schedule, byte[] payload,
        MessagePublisher publisher, long startedAt) {
      this.command = command;
      this.schedule = schedule;
      this.payload = payload;
      this.publisher = publisher;
      this.startedAt = startedAt;
    }
  }
}
