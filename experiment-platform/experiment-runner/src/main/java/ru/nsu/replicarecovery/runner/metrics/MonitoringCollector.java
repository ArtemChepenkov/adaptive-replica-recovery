package ru.nsu.replicarecovery.runner.metrics;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import ru.nsu.replicarecovery.runner.client.PrometheusClient;

/** Three read-only sources share a bounded collection budget and a five-second cache. */
public final class MonitoringCollector implements PrometheusClient, AutoCloseable {
  private final Set<Integer> expected;
  private final Clock clock;
  private final Duration cacheDuration;
  private final Supplier<PrometheusMonitoringSource.Reading> prometheus;
  private final Supplier<DockerStatsSource.Reading> docker;
  private final Supplier<String> executor;
  private final ExecutorService readers = Executors.newFixedThreadPool(3, task -> {
    Thread thread = new Thread(task, "monitoring-reader"); thread.setDaemon(true); return thread;
  });
  private MonitoringSnapshot cached;
  private long cachedAt;

  public MonitoringCollector(String prometheusUrl, String cruiseUrl, String composeProject,
      Set<Integer> expectedBrokers, Clock clock) {
    this(expectedBrokers, clock, Duration.ofSeconds(5),
        new PrometheusMonitoringSource(prometheusUrl, expectedBrokers, clock),
        new DockerStatsSource(composeProject, expectedBrokers, clock), new CruiseControlMonitoringSource(cruiseUrl));
  }
  MonitoringCollector(Set<Integer> expected, Clock clock, Duration cacheDuration,
      Supplier<PrometheusMonitoringSource.Reading> prometheus, Supplier<DockerStatsSource.Reading> docker,
      Supplier<String> executor) {
    if (expected.isEmpty() || expected.stream().anyMatch(id -> id == null || id <= 0) || cacheDuration.isNegative()) {
      throw new IllegalArgumentException("positive expected broker IDs and nonnegative cache duration required");
    }
    this.expected = Set.copyOf(expected); this.clock = clock; this.cacheDuration = cacheDuration;
    this.prometheus = prometheus; this.docker = docker; this.executor = executor;
  }

  public synchronized MonitoringSnapshot snapshot() {
    long now = System.nanoTime();
    if (cached != null && now - cachedAt < cacheDuration.toNanos()) return cached;
    long deadline = now + TimeUnit.SECONDS.toNanos(3);
    Future<PrometheusMonitoringSource.Reading> promFuture = readers.submit((Callable<PrometheusMonitoringSource.Reading>) prometheus::get);
    Future<DockerStatsSource.Reading> dockerFuture = readers.submit((Callable<DockerStatsSource.Reading>) docker::get);
    Future<String> executorFuture = readers.submit((Callable<String>) executor::get);
    List<String> errors = new ArrayList<>();
    var prom = await(promFuture, deadline, "PROMETHEUS_UNAVAILABLE", errors);
    var containers = await(dockerFuture, deadline, "DOCKER_UNAVAILABLE", errors);
    var state = await(executorFuture, deadline, "CRUISE_CONTROL_UNAVAILABLE", errors);
    if (prom != null) errors.addAll(prom.errors());
    if (containers != null) errors.addAll(containers.errors());
    Map<Integer, Double> cpu = containers == null ? Map.of() : containers.cpuPercent();
    Map<Integer, Double> memory = containers == null ? Map.of() : containers.memoryBytes();
    Map<Integer, Double> disk = containers == null ? Map.of() : containers.diskWriteBytesPerSecond();
    Map<Integer, Double> network = prom == null ? Map.of() : prom.networkOutBytesPerSecond();
    boolean available = cpu.keySet().equals(expected) && memory.keySet().equals(expected)
        && disk.keySet().equals(expected) && network.keySet().equals(expected) && state != null
        && prom != null && prom.underReplicatedPartitions() != null && prom.executorReplicaActions() != null
        && errors.isEmpty();
    cached = new MonitoringSnapshot(clock.instant(), available, cpu, memory, network, disk,
        prom == null ? null : prom.underReplicatedPartitions(), prom == null ? null : prom.executorReplicaActions(), state,
        List.copyOf(errors));
    cachedAt = System.nanoTime();
    return cached;
  }

  @Override public ResourceMetrics current() {
    MonitoringSnapshot value = snapshot();
    return new ResourceMetrics(aggregate(value.brokerCpuPercent(), true),
        aggregate(value.brokerNetworkOutBytesPerSecond(), false), aggregate(value.brokerDiskWriteBytesPerSecond(), false), value);
  }
  private Double aggregate(Map<Integer, Double> values, boolean average) {
    if (!values.keySet().equals(expected)) return null;
    double sum = values.values().stream().mapToDouble(Double::doubleValue).sum();
    return average ? sum / values.size() : sum;
  }
  private static <T> T await(Future<T> future, long deadline, String failure, List<String> errors) {
    try { return future.get(Math.max(1, deadline - System.nanoTime()), TimeUnit.NANOSECONDS); }
    catch (Exception exception) {
      future.cancel(true);
      if (exception instanceof InterruptedException) Thread.currentThread().interrupt();
      errors.add(failure); return null;
    }
  }
  @Override public void close() { readers.shutdownNow(); }
}
