package ru.nsu.replicarecovery.load.metrics;

import jakarta.annotation.PreDestroy;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import ru.nsu.replicarecovery.load.core.LoadCommand;
import ru.nsu.replicarecovery.load.core.MessagePublisher;
import ru.nsu.replicarecovery.load.core.OpenLoopLoadEngine;
import ru.nsu.replicarecovery.load.core.RateSchedule;
import ru.nsu.replicarecovery.load.kafka.KafkaMessagePublisher;
import ru.nsu.replicarecovery.load.storage.RawRequestLog;
import ru.nsu.replicarecovery.model.ControlSettingsSnapshot;
import ru.nsu.replicarecovery.model.LoadMetricsSnapshot;

@Service
public final class LoadMetricsService implements AutoCloseable {
  private final Supplier<MessagePublisher> publishers;
  private final Path outputDirectory;
  private final Clock clock;
  private final BiFunction<Path, Consumer<RuntimeException>, RawRequestLog> logs;
  private final Duration stopTimeout;
  private final ExecutorService finalizer = Executors.newSingleThreadExecutor(action -> {
    Thread thread = new Thread(action, "load-run-finalizer");
    thread.setDaemon(true);
    return thread;
  });
  private final ScheduledExecutorService sampler = Executors.newSingleThreadScheduledExecutor(action -> {
    Thread thread = new Thread(action, "load-metrics-sampler");
    thread.setDaemon(true);
    return thread;
  });
  private volatile Session active;
  private boolean closed;

  @Autowired
  public LoadMetricsService(@Value("${load.bootstrap-servers}") String servers,
      @Value("${load.topic}") String topic, @Value("${load.output-directory}") String output) {
    this(() -> new KafkaMessagePublisher(servers, topic), Path.of(output), Clock.systemUTC());
  }

  public LoadMetricsService(Supplier<MessagePublisher> publishers, Path output, Clock clock) {
    this(publishers, output, clock, RawRequestLog::new);
  }

  LoadMetricsService(Supplier<MessagePublisher> publishers, Path output, Clock clock,
      BiFunction<Path, Consumer<RuntimeException>, RawRequestLog> logs) {
    this(publishers, output, clock, logs, Duration.ofSeconds(10));
  }

  LoadMetricsService(Supplier<MessagePublisher> publishers, Path output, Clock clock,
      BiFunction<Path, Consumer<RuntimeException>, RawRequestLog> logs, Duration stopTimeout) {
    this.publishers = publishers;
    this.outputDirectory = output.toAbsolutePath().normalize();
    this.clock = clock;
    this.logs = logs;
    this.stopTimeout = stopTimeout;
    sampler.scheduleAtFixedRate(() -> {
      Session session = active;
      if (session != null && !session.completed.isDone()) {
        try {
          sample(session);
        } catch (RuntimeException exception) {
          fail(session, exception);
        }
      }
    }, 1, 1, TimeUnit.SECONDS);
  }

  public synchronized LoadStatus start(LoadCommand command, ControlSettingsSnapshot settings) {
    if (closed || (active != null && !active.completed.isDone())) {
      throw new RunConflictException("a previous run is active or cleanup has not finished");
    }
    Session session = new Session(command, settings, clock.instant(), outputDirectory);
    active = session;
    try {
      session.raw = logs.apply(session.rawPath, error -> fail(session, error));
      session.engine = new OpenLoopLoadEngine(publishers, observation -> {
        session.raw.record(observation);
        session.window.record(observation);
        session.count.incrementAndGet();
        if (!observation.success()) {
          session.errors.incrementAndGet();
        }
      }, clock);
      synchronized (session.lifecycle) {
        if (session.failure.get() != null) {
          throw new IllegalStateException("request log initialization failed", session.failure.get());
        }
        session.engine.start(command);
        session.startedAt = clock.instant();
        session.state = "RUNNING";
      }
      session.engine.completion().whenCompleteAsync((ignored, error) -> finish(session, error), finalizer);
      sample(session);
      return status(session);
    } catch (RuntimeException exception) {
      fail(session, exception);
      if (session.engine == null || !session.engine.isRunning()) {
        CompletableFuture.runAsync(() -> finish(session, exception), finalizer);
      } else {
        session.engine.completion().whenCompleteAsync((ignored, error) -> finish(session, error), finalizer);
      }
      throw exception;
    }
  }

  public LoadStatus stop() {
    long stopStarted = System.nanoTime();
    Session session;
    synchronized (this) {
      session = active;
    }
    if (session == null) {
      return status();
    }
    boolean finished;
    synchronized (session.lifecycle) {
      finished = session.completed.isDone();
      if (!finished) {
        session.stopRequested = true;
        if (session.failure.get() == null) {
          session.state = "STOPPING";
        }
      }
    }
    if (finished) {
      return status(session);
    }
    if (session.engine == null) {
      // A failed log constructor is being finalized asynchronously.
      return status(session);
    }
    try {
      session.engine.stop();
    } catch (RuntimeException exception) {
      fail(session, exception);
    }
    // The completion handler closes the log after all observations and the producer.
    if (session.engine.completion().isDone()) {
      CompletableFuture.runAsync(() -> finish(session, null), finalizer);
    }
    try {
      long remaining = Math.max(0, stopTimeout.toNanos() - (System.nanoTime() - stopStarted));
      session.completed.get(remaining, TimeUnit.NANOSECONDS);
    } catch (TimeoutException exception) {
      // Keep cleanup in progress and reject a new start until it really finishes.
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
      fail(session, exception);
    } catch (ExecutionException exception) {
      fail(session, exception.getCause());
    }
    return status(session);
  }

  public LoadStatus status() {
    Session session = active;
    return session == null ? new LoadStatus("IDLE", null, null, null, 0, 0, 0, null, null, true)
        : status(session);
  }

  public LoadMetricsSnapshot current() {
    Session session = active;
    return session == null
        ? new SlidingLatencyWindow(Duration.ofSeconds(60)).snapshot(clock.instant(), 0, 0)
        : snapshot(session);
  }

  public List<LoadMetricsSnapshot> history() {
    Session session = active;
    if (session == null) {
      return List.of();
    }
    synchronized (session.history) {
      return session.history.stream().sorted(Comparator.comparing(LoadMetricsSnapshot::generatedAt)).toList();
    }
  }

  public CompletableFuture<LoadStatus> completion() {
    Session session = active;
    return session == null ? CompletableFuture.completedFuture(status()) : session.completed.copy();
  }

  @PreDestroy
  @Override
  public void close() {
    synchronized (this) {
      if (closed) {
        return;
      }
      closed = true;
    }
    stop();
    sampler.shutdownNow();
    Session session = active;
    if (session == null) {
      finalizer.shutdown();
    } else {
      session.completed.whenComplete((ignored, error) -> finalizer.shutdown());
    }
  }

  private LoadStatus status(Session session) {
    return status(session, session.completed.isDone());
  }

  private LoadStatus status(Session session, boolean cleanupComplete) {
    Throwable failure = session.failure.get();
    return new LoadStatus(session.state, session.id, session.startedAt, session.finishedAt,
        session.count.get(), session.errors.get(),
        session.engine == null ? 0 : session.engine.outstandingRequests(), session.rawPath.toString(),
        failure == null ? null : failure.getClass().getSimpleName() + ": " + failure.getMessage(),
        cleanupComplete);
  }

  private LoadMetricsSnapshot snapshot(Session session) {
    Instant end = session.finishedAt == null ? clock.instant() : session.finishedAt;
    Duration elapsed = Duration.between(session.startedAt, end);
    long target = session.state.equals("RUNNING")
        ? session.schedule.targetRps(elapsed.isNegative() ? Duration.ZERO : elapsed) : 0;
    long outstanding = session.engine == null ? 0 : session.engine.outstandingRequests();
    return session.window.snapshot(end, target, outstanding, session.settings);
  }

  private void sample(Session session) {
    LoadMetricsSnapshot snapshot = snapshot(session);
    synchronized (session.history) {
      session.history.add(snapshot);
    }
  }

  private void fail(Session session, Throwable error) {
    boolean firstFailure;
    synchronized (session.lifecycle) {
      if (session.completed.isDone()) {
        return;
      }
      firstFailure = session.failure.compareAndSet(null, error);
      session.state = "FAILED";
    }
    OpenLoopLoadEngine engine = session.engine;
    if (firstFailure && engine != null && !session.completed.isDone()) {
      CompletableFuture.runAsync(() -> {
        try {
          engine.stop();
        } catch (RuntimeException ignored) {
          // The first failure is retained; finalization is tied to engine completion.
        }
      }, finalizer);
    }
  }

  private void finish(Session session, Throwable error) {
    if (!session.finishing.compareAndSet(false, true)) {
      return;
    }
    if (error != null) {
      session.failure.compareAndSet(null, error);
    }
    try {
      if (session.raw != null) {
        session.raw.close();
      }
    } catch (RuntimeException exception) {
      session.failure.compareAndSet(null, exception);
    }
    try {
      if (session.engine != null) {
        session.engine.close();
      }
    } catch (RuntimeException exception) {
      session.failure.compareAndSet(null, exception);
    }
    session.finishedAt = clock.instant();
    sample(session);
    synchronized (session.lifecycle) {
      session.state = session.failure.get() != null ? "FAILED"
          : session.stopRequested ? "STOPPED" : "COMPLETED";
      session.completed.complete(status(session, true));
    }
  }

  public record LoadStatus(String state, String runId, Instant startedAt, Instant finishedAt,
      long requestCount, long errorCount, long outstandingRequests, String rawLogPath,
      String error, boolean cleanupComplete) { }

  public static final class RunConflictException extends IllegalStateException {
    public RunConflictException(String message) {
      super(message);
    }
  }

  private static final class Session {
    final String id = UUID.randomUUID().toString();
    volatile Instant startedAt;
    final ControlSettingsSnapshot settings;
    final RateSchedule schedule;
    final SlidingLatencyWindow window;
    final Path rawPath;
    final AtomicLong count = new AtomicLong();
    final AtomicLong errors = new AtomicLong();
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final AtomicBoolean finishing = new AtomicBoolean();
    final Object lifecycle = new Object();
    final CompletableFuture<LoadStatus> completed = new CompletableFuture<>();
    final List<LoadMetricsSnapshot> history = new ArrayList<>();
    volatile String state = "STARTING";
    volatile Instant finishedAt;
    volatile boolean stopRequested;
    volatile OpenLoopLoadEngine engine;
    RawRequestLog raw;

    Session(LoadCommand command, ControlSettingsSnapshot settings, Instant startedAt, Path output) {
      this.startedAt = startedAt;
      this.settings = settings;
      schedule = RateSchedule.of(command.load().schedule());
      window = new SlidingLatencyWindow(settings.recovery().metricsWindow());
      rawPath = output.resolve(id).resolve("requests.csv");
    }
  }
}
