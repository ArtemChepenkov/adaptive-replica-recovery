package ru.nsu.replicarecovery.runner.client;

public interface PrometheusClient {
  ResourceMetrics current();
  record ResourceMetrics(Double brokerCpuPercent, Double brokerNetworkOutBytesPerSecond,
      Double brokerDiskWriteBytesPerSecond) {
    public ResourceMetrics {
      for (Double value : new Double[] {brokerCpuPercent, brokerNetworkOutBytesPerSecond, brokerDiskWriteBytesPerSecond}) {
        if (value != null && (!Double.isFinite(value) || value < 0)) {
          throw new IllegalArgumentException("resource metrics must be finite and non-negative");
        }
      }
    }
  }
}
