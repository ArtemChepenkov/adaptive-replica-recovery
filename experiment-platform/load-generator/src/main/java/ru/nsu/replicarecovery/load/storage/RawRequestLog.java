package ru.nsu.replicarecovery.load.storage;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import ru.nsu.replicarecovery.load.metrics.RequestObservation;

/** One serialized writer for all request callbacks of a run. */
public final class RawRequestLog implements AutoCloseable {
  static final String HEADER = "timestampEpochMs,sequence,latencyMicros,success,errorClass,bytes\n";
  private final Object monitor = new Object();
  private final BufferedWriter writer;
  private final Consumer<RuntimeException> onFailure;
  private final ScheduledExecutorService scheduler;
  private final AtomicBoolean failureNotified = new AtomicBoolean();
  private RuntimeException failure;
  private boolean closed;

  public RawRequestLog(Path path, Consumer<RuntimeException> onFailure) {
    this(open(path, onFailure), onFailure, newScheduler());
  }

  RawRequestLog(BufferedWriter writer, Consumer<RuntimeException> onFailure,
      ScheduledExecutorService scheduler) {
    this.writer = Objects.requireNonNull(writer);
    this.onFailure = Objects.requireNonNull(onFailure);
    this.scheduler = Objects.requireNonNull(scheduler);
    try {
      writer.write(HEADER);
      writer.flush();
      scheduler.scheduleAtFixedRate(this::flushScheduled, 1, 1, TimeUnit.SECONDS);
    } catch (IOException exception) {
      failConstruction(new UncheckedIOException(exception));
    } catch (RuntimeException exception) {
      failConstruction(exception);
    }
  }

  public void record(RequestObservation observation) {
    Objects.requireNonNull(observation);
    String row = observation.completedAt().toEpochMilli() + "," + observation.sequence() + ","
        + observation.latencyMicros() + "," + observation.success() + ","
        + csvField(observation.errorClass()) + "," + observation.bytes() + "\n";
    write(() -> writer.write(row));
  }

  public void flush() {
    write(writer::flush);
  }

  @Override
  public void close() {
    scheduler.shutdownNow();
    RuntimeException problem;
    synchronized (monitor) {
      if (closed) {
        return;
      }
      closed = true;
      problem = failure;
      try {
        writer.flush();
      } catch (IOException exception) {
        problem = rememberFailure(exception);
      } finally {
        try {
          writer.close();
        } catch (IOException exception) {
          problem = rememberFailure(exception);
        }
      }
    }
    if (problem != null) {
      notifyFailure(problem);
      throw problem;
    }
  }

  private void write(IoAction action) {
    RuntimeException problem;
    synchronized (monitor) {
      if (closed) {
        throw new IllegalStateException("request log is closed");
      }
      problem = failure;
      if (problem == null) {
        try {
          action.run();
        } catch (IOException exception) {
          problem = rememberFailure(exception);
        }
      }
    }
    if (problem != null) {
      notifyFailure(problem);
      throw problem;
    }
  }

  private RuntimeException rememberFailure(IOException exception) {
    if (failure == null) {
      failure = new UncheckedIOException(exception);
    } else {
      failure.addSuppressed(exception);
    }
    return failure;
  }

  private void notifyFailure(RuntimeException problem) {
    if (failureNotified.compareAndSet(false, true)) {
      notifyFailure(onFailure, problem);
    }
  }

  private static void notifyFailure(Consumer<RuntimeException> callback, RuntimeException problem) {
    try {
      callback.accept(problem);
    } catch (RuntimeException callbackFailure) {
      if (callbackFailure != problem) {
        problem.addSuppressed(callbackFailure);
      }
    }
  }

  private void flushScheduled() {
    synchronized (monitor) {
      if (closed) {
        return;
      }
    }
    try {
      flush();
    } catch (RuntimeException ignored) {
      // write() has already reported I/O failures. A concurrent close is harmless.
    }
  }

  private void failConstruction(RuntimeException problem) {
    scheduler.shutdownNow();
    try {
      writer.close();
    } catch (IOException exception) {
      problem.addSuppressed(exception);
    }
    notifyFailure(problem);
    throw problem;
  }

  private static BufferedWriter open(Path path, Consumer<RuntimeException> onFailure) {
    Objects.requireNonNull(path);
    Objects.requireNonNull(onFailure);
    try {
      Path parent = path.toAbsolutePath().getParent();
      if (parent != null) {
        Files.createDirectories(parent);
      }
      return Files.newBufferedWriter(path, StandardCharsets.UTF_8, StandardOpenOption.CREATE_NEW,
          StandardOpenOption.WRITE);
    } catch (IOException exception) {
      RuntimeException problem = new UncheckedIOException(exception);
      notifyFailure(onFailure, problem);
      throw problem;
    }
  }

  private static ScheduledExecutorService newScheduler() {
    return Executors.newSingleThreadScheduledExecutor(action -> {
      Thread thread = new Thread(action, "raw-request-log-flush");
      thread.setDaemon(true);
      return thread;
    });
  }

  private static String csvField(String value) {
    if (value == null) {
      return "";
    }
    if (value.indexOf(',') >= 0 || value.indexOf('"') >= 0 || value.indexOf('\n') >= 0
        || value.indexOf('\r') >= 0) {
      return "\"" + value.replace("\"", "\"\"") + "\"";
    }
    return value;
  }

  @FunctionalInterface
  private interface IoAction {
    void run() throws IOException;
  }
}
