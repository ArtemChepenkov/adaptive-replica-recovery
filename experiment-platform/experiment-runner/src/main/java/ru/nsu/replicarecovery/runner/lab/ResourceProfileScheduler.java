package ru.nsu.replicarecovery.runner.lab;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import ru.nsu.replicarecovery.model.ResourceProfileChange;

/** The experiment worker applies predefined changes; no background mutator survives cleanup. */
public final class ResourceProfileScheduler {
  private final List<ResourceProfileChange> changes;
  private final Consumer<String> apply;
  private final LongSupplier nanos;
  private boolean started;
  private boolean cancelled;
  private long startedAt;
  private int next;

  public ResourceProfileScheduler(List<ResourceProfileChange> changes, Consumer<String> apply, LongSupplier nanos) {
    if (changes == null || apply == null || nanos == null) throw new IllegalArgumentException("schedule arguments required");
    long previous = 0;
    for (var change : changes) {
      if (change == null || change.at() == null || change.at().isNegative() || change.profile() == null
          || !change.profile().matches("[a-z0-9][a-z0-9-]{0,63}")) throw new IllegalArgumentException("invalid profile schedule");
      long offset;
      try { offset = change.at().toNanos(); }
      catch (ArithmeticException exception) { throw new IllegalArgumentException("profile schedule exceeds nanosecond range", exception); }
      if (offset < previous) throw new IllegalArgumentException("profile schedule must be ordered");
      previous = offset;
    }
    this.changes = List.copyOf(changes); this.apply = apply; this.nanos = nanos;
  }
  public void start() {
    if (started || cancelled) throw new IllegalStateException("profile schedule cannot be restarted");
    startedAt = nanos.getAsLong(); started = true;
  }
  public void tick() {
    if (!started || cancelled) return;
    long elapsed = nanos.getAsLong() - startedAt;
    if (elapsed < 0) throw new IllegalStateException("monotonic clock moved backwards");
    while (next < changes.size() && changes.get(next).at().toNanos() <= elapsed) {
      var change = changes.get(next++);
      try { apply.accept(change.profile()); }
      catch (RuntimeException exception) { cancelled = true; throw exception; }
    }
  }
  public void cancel() { cancelled = true; }
}
