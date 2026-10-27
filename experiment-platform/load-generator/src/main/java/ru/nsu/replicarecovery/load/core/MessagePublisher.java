package ru.nsu.replicarecovery.load.core;

import java.time.Duration;
import java.util.function.Consumer;

public interface MessagePublisher {
  /** Must return promptly; callback receives null on success, an exception on failure. */
  void send(long seed, long sequence, byte[] payload, Consumer<Exception> callback);

  void close(Duration timeout);
}
