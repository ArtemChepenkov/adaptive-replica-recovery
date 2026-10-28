package ru.nsu.replicarecovery.load.metrics;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import org.HdrHistogram.Histogram;
import ru.nsu.replicarecovery.model.ControlSettingsSnapshot;
import ru.nsu.replicarecovery.model.LoadMetricsSnapshot;

/** A time-bounded window of completions; percentile values cover successful writes only. */
public final class SlidingLatencyWindow {
  private final long windowMillis;
  private final NavigableMap<Long, Bucket> buckets = new TreeMap<>();
  private long latestMillis = Long.MIN_VALUE;

  public SlidingLatencyWindow(Duration window) {
    windowMillis = window.toMillis();
    if (windowMillis <= 0) {
      throw new IllegalArgumentException("metrics window must be at least one millisecond");
    }
  }

  public synchronized void record(RequestObservation observation) {
    long timestamp = observation.completedAt().toEpochMilli();
    latestMillis = Math.max(latestMillis, timestamp);
    discardBefore(latestMillis - windowMillis);
    if (timestamp < latestMillis - windowMillis) {
      return;
    }
    Bucket bucket = buckets.computeIfAbsent(Math.floorDiv(timestamp, 1000), ignored -> new Bucket());
    bucket.observations.add(observation);
    bucket.count++;
    if (observation.success()) {
      bucket.latencies.recordValue(observation.latencyMicros());
      bucket.bytes += observation.bytes();
    } else {
      bucket.errors++;
    }
  }

  public LoadMetricsSnapshot snapshot(Instant end, long offeredRps, long outstanding) {
    return snapshot(end, offeredRps, outstanding, null);
  }

  public synchronized LoadMetricsSnapshot snapshot(Instant end, long offeredRps, long outstanding,
      ControlSettingsSnapshot settings) {
    long endMillis = end.toEpochMilli();
    long startMillis = endMillis - windowMillis;
    discardBefore(startMillis);
    Histogram histogram = new Histogram(3);
    long count = 0;
    long errors = 0;
    long bytes = 0;
    for (var entry : buckets.subMap(Math.floorDiv(startMillis, 1000), true,
        Math.floorDiv(endMillis, 1000), true).entrySet()) {
      Bucket bucket = entry.getValue();
      long firstMillis = entry.getKey() * 1000;
      if (firstMillis >= startMillis && firstMillis + 999 <= endMillis) {
        histogram.add(bucket.latencies);
        count += bucket.count;
        errors += bucket.errors;
        bytes += bucket.bytes;
      } else {
        // Full seconds use merged histograms; only the two partial boundary seconds
        // need individual completions to match the millisecond CSV exactly.
        for (RequestObservation observation : bucket.observations) {
          long timestamp = observation.completedAt().toEpochMilli();
          if (timestamp >= startMillis && timestamp <= endMillis) {
            count++;
            if (observation.success()) {
              histogram.recordValue(observation.latencyMicros());
              bytes += observation.bytes();
            } else {
              errors++;
            }
          }
        }
      }
    }
    return new LoadMetricsSnapshot(Instant.ofEpochMilli(startMillis), Instant.ofEpochMilli(endMillis),
        end, count, percentile(histogram, 50), percentile(histogram, 95), percentile(histogram, 99),
        errors, count == 0 ? 0 : (double) errors / count, offeredRps,
        (count - errors) * 1000.0 / windowMillis, bytes, outstanding, settings);
  }

  private void discardBefore(long cutoffMillis) {
    buckets.headMap(Math.floorDiv(cutoffMillis, 1000), false).clear();
  }

  private static Double percentile(Histogram histogram, double rank) {
    return histogram.getTotalCount() == 0 ? null : histogram.getValueAtPercentile(rank) / 1000.0;
  }

  private static final class Bucket {
    final Histogram latencies = new Histogram(3);
    final List<RequestObservation> observations = new ArrayList<>();
    long count;
    long errors;
    long bytes;
  }
}
