package ru.nsu.replicarecovery.runner.client;

import com.fasterxml.jackson.databind.JsonNode;

/** Queries are configurable contracts for Task 8; absent series stay null. */
public final class HttpPrometheusClient implements PrometheusClient {
  private final HttpJsonTransport http;
  private final String cpuQuery;
  private final String networkQuery;
  private final String diskQuery;

  public HttpPrometheusClient(String baseUrl) {
    this(baseUrl, "avg(broker_cpu_percent)", "sum(rate(broker_network_out_bytes_total[1m]))",
        "sum(rate(broker_disk_write_bytes_total[1m]))");
  }

  public HttpPrometheusClient(String baseUrl, String cpuQuery, String networkQuery, String diskQuery) {
    this(new HttpJsonTransport(baseUrl), cpuQuery, networkQuery, diskQuery);
  }

  HttpPrometheusClient(HttpJsonTransport http, String cpuQuery, String networkQuery, String diskQuery) {
    this.http = http;
    this.cpuQuery = cpuQuery;
    this.networkQuery = networkQuery;
    this.diskQuery = diskQuery;
  }

  @Override
  public ResourceMetrics current() {
    return new ResourceMetrics(query(cpuQuery), query(networkQuery), query(diskQuery));
  }

  private Double query(String query) {
    JsonNode response = http.get("/api/v1/query?query=" + HttpJsonTransport.query(query));
    if (!"success".equals(HttpJsonTransport.text(response, "status"))) {
      throw new RemoteCallException("Prometheus query unsuccessful", 200);
    }
    JsonNode data = HttpJsonTransport.required(response, "data");
    if (!"vector".equals(HttpJsonTransport.text(data, "resultType"))) {
      throw new RemoteCallException("Prometheus instant query must return a vector", 200);
    }
    JsonNode result = HttpJsonTransport.required(data, "result");
    if (!result.isArray() || result.size() > 1) {
      throw new RemoteCallException("Prometheus aggregate query must return at most one value", 200);
    }
    if (result.isEmpty()) { return null; }
    JsonNode value = HttpJsonTransport.required(result.get(0), "value");
    if (!value.isArray() || value.size() != 2 || !value.get(1).isTextual()) {
      throw new RemoteCallException("invalid Prometheus sample", 200);
    }
    try {
      double number = Double.parseDouble(value.get(1).textValue());
      if (!Double.isFinite(number) || number < 0) { throw new NumberFormatException("negative or non-finite"); }
      return number;
    } catch (NumberFormatException exception) {
      throw new RemoteCallException("invalid Prometheus numeric value", exception);
    }
  }
}
