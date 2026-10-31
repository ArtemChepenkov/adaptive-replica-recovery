package ru.nsu.replicarecovery.runner.client;

import ru.nsu.replicarecovery.runner.metrics.MonitoringSnapshot;

public interface PrometheusClient {
  ResourceMetrics current();
  record ResourceMetrics(Double brokerCpuPercent, Double brokerNetworkOutBytesPerSecond,
      Double brokerDiskWriteBytesPerSecond, MonitoringSnapshot monitoring) {
    public ResourceMetrics(Double cpu, Double network, Double disk) { this(cpu, network, disk, null); }
    public ResourceMetrics {
      for (Double value : new Double[] {brokerCpuPercent, brokerNetworkOutBytesPerSecond, brokerDiskWriteBytesPerSecond}) {
        if (value != null && (!Double.isFinite(value) || value < 0)) {
          throw new IllegalArgumentException("resource metrics must be finite and non-negative");
        }
      }
    }
  }
}
