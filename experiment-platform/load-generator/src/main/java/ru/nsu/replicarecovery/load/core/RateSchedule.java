package ru.nsu.replicarecovery.load.core;

import java.math.BigInteger;
import java.time.Duration;
import java.util.List;
import ru.nsu.replicarecovery.model.LoadSegment;

/** All load modes arrive as segments; each segment occupies [start, end). */
public final class RateSchedule {
  private static final long NANOS_PER_SECOND = 1_000_000_000L;
  private static final BigInteger BILLION = BigInteger.valueOf(NANOS_PER_SECOND);
  private final List<LoadSegment> segments;
  private final long durationNanos;

  private RateSchedule(List<LoadSegment> segments) {
    if (segments == null || segments.isEmpty()) {
      throw new IllegalArgumentException("load schedule must not be empty");
    }
    this.segments = List.copyOf(segments);
    long total = 0;
    for (LoadSegment segment : segments) {
      if (segment.duration() == null || segment.duration().isNegative()
          || segment.duration().isZero()) {
        throw new IllegalArgumentException("segment duration must be positive");
      }
      if (segment.requestsPerSecond() <= 0 || segment.requestsPerSecond() > NANOS_PER_SECOND) {
        throw new IllegalArgumentException("requestsPerSecond must be in [1, 1000000000]");
      }
      try {
        total = Math.addExact(total, segment.duration().toNanos());
      } catch (ArithmeticException exception) {
        throw new IllegalArgumentException("schedule duration exceeds nanosecond range", exception);
      }
    }
    durationNanos = total;
  }

  public static RateSchedule of(List<LoadSegment> segments) {
    return new RateSchedule(segments);
  }

  public long targetRps(Duration elapsed) {
    if (elapsed.isNegative()) {
      throw new IllegalArgumentException("elapsed time must not be negative");
    }
    for (LoadSegment segment : segments) {
      if (elapsed.compareTo(segment.duration()) < 0) {
        return segment.requestsPerSecond();
      }
      elapsed = elapsed.minus(segment.duration());
    }
    return 0;
  }

  public boolean isFinished(Duration elapsed) {
    return targetRps(elapsed) == 0;
  }

  public Duration duration() {
    return Duration.ofNanos(durationNanos);
  }

  // Emit at each whole request of integrated demand, starting with request 0.
  // Exact arithmetic carries fractional demand across segment boundaries.
  long deadlineNanos(long sequence) {
    long start = 0;
    BigInteger work = BigInteger.ZERO;
    BigInteger target = BigInteger.valueOf(sequence).multiply(BILLION);
    for (LoadSegment segment : segments) {
      long duration = segment.duration().toNanos();
      BigInteger rate = BigInteger.valueOf(segment.requestsPerSecond());
      BigInteger endWork = work.add(BigInteger.valueOf(duration).multiply(rate));
      if (target.compareTo(endWork) < 0) {
        BigInteger[] offset = target.subtract(work).divideAndRemainder(rate);
        return start + offset[0].longValueExact() + (offset[1].signum() == 0 ? 0 : 1);
      }
      work = endWork;
      start += duration;
    }
    return durationNanos;
  }
}
