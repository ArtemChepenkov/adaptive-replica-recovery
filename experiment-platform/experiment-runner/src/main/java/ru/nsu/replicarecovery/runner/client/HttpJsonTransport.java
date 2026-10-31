package ru.nsu.replicarecovery.runner.client;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** Bounded complete-body reads. Only GET is retried, with three total attempts. */
public final class HttpJsonTransport {
  final ObjectMapper json = JsonMapper.builder().findAndAddModules()
      .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
      .disable(SerializationFeature.WRITE_DURATIONS_AS_TIMESTAMPS)
      .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
      .disable(DeserializationFeature.ACCEPT_FLOAT_AS_INT)
      .disable(MapperFeature.ALLOW_COERCION_OF_SCALARS)
      .enable(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES).build();
  private final String base;
  private final HttpClient client;
  private final Duration readTimeout;
  private final Sleeper sleeper;
  private final int getAttempts;

  HttpJsonTransport(String base) {
    this(base, Duration.ofSeconds(3), Duration.ofSeconds(10), duration ->
        TimeUnit.NANOSECONDS.sleep(duration.toNanos()));
  }

  HttpJsonTransport(String base, Duration connectTimeout, Duration readTimeout, Sleeper sleeper) {
    this(base, connectTimeout, readTimeout, sleeper, 3);
  }

  private HttpJsonTransport(String base, Duration connectTimeout, Duration readTimeout, Sleeper sleeper, int getAttempts) {
    URI uri = URI.create(base);
    if ((!"http".equals(uri.getScheme()) && !"https".equals(uri.getScheme()))
        || uri.getHost() == null || uri.getRawUserInfo() != null
        || uri.getQuery() != null || uri.getFragment() != null
        || connectTimeout.isNegative() || connectTimeout.isZero()
        || readTimeout.isNegative() || readTimeout.isZero()) {
      throw new IllegalArgumentException("an HTTP base URL and positive timeouts are required");
    }
    this.base = base.endsWith("/") ? base.substring(0, base.length() - 1) : base;
    this.readTimeout = readTimeout;
    this.sleeper = sleeper;
    this.getAttempts = getAttempts;
    client = HttpClient.newBuilder().connectTimeout(connectTimeout)
        .followRedirects(HttpClient.Redirect.NEVER).build();
  }

  public static HttpJsonTransport monitoring(String base) {
    return new HttpJsonTransport(base, Duration.ofSeconds(1), Duration.ofSeconds(2), duration -> { }, 1);
  }

  public JsonNode get(String path) {
    return exchange("GET", path, null).body();
  }

  Reply post(String path, Object value) {
    byte[] body;
    try {
      body = value == null ? new byte[0] : json.writeValueAsBytes(value);
    } catch (IOException exception) {
      throw new IllegalArgumentException("cannot encode HTTP request", exception);
    }
    return exchange("POST", path, body);
  }

  private Reply exchange(String method, String path, byte[] body) {
    int attempts = method.equals("GET") ? getAttempts : 1;
    for (int attempt = 0; attempt < attempts; attempt++) {
      if (Thread.currentThread().isInterrupted()) {
        throw new RemoteCallException("HTTP call interrupted", new InterruptedException());
      }
      HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(base + path))
          .timeout(readTimeout).header("Accept", "application/json");
      if (method.equals("POST")) {
        request.header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofByteArray(body));
      } else {
        request.GET();
      }
      var future = client.sendAsync(request.build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
      HttpResponse<String> response;
      try {
        // HttpRequest.timeout alone is not a complete-body read deadline.
        response = future.get(readTimeout.toNanos(), TimeUnit.NANOSECONDS);
      } catch (InterruptedException exception) {
        future.cancel(true);
        Thread.currentThread().interrupt();
        throw new RemoteCallException("HTTP call interrupted", exception);
      } catch (TimeoutException | ExecutionException exception) {
        future.cancel(true);
        if (attempt + 1 < attempts) {
          pause(attempt);
          continue;
        }
        throw new RemoteCallException("HTTP " + method + " failed after " + attempts + " attempt(s)", exception);
      }
      int status = response.statusCode();
      if (status < 200 || status >= 300) {
        if (attempt + 1 < attempts && (status == 408 || status == 429 || status >= 500)) {
          pause(attempt);
          continue;
        }
        throw new RemoteCallException("HTTP " + method + " returned " + status, status);
      }
      try {
        JsonNode node = response.body().isBlank() ? json.createObjectNode() : json.readTree(response.body());
        if (node == null || !node.isObject()) {
          throw new RemoteCallException("HTTP response must be a JSON object", status);
        }
        return new Reply(node, response.headers());
      } catch (IOException exception) {
        throw new RemoteCallException("invalid HTTP JSON response", exception);
      }
    }
    throw new IllegalStateException("HTTP retry loop exhausted unexpectedly");
  }

  private void pause(int failedAttempt) {
    try {
      sleeper.sleep(Duration.ofSeconds(1L << failedAttempt));
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
      throw new RemoteCallException("HTTP retry interrupted", exception);
    }
  }

  public static String query(String value) {
    return URLEncoder.encode(value, StandardCharsets.UTF_8);
  }

  static JsonNode required(JsonNode node, String field) {
    JsonNode value = node.get(field);
    if (value == null || value.isNull()) {
      throw new RemoteCallException("missing response field: " + field, 200);
    }
    return value;
  }

  static String text(JsonNode node, String field) {
    JsonNode value = required(node, field);
    if (!value.isTextual() || value.textValue().isBlank()) {
      throw new RemoteCallException("invalid text response field: " + field, 200);
    }
    return value.textValue();
  }

  static long nonnegativeLong(JsonNode node, String field) {
    JsonNode value = required(node, field);
    if (!value.isIntegralNumber() || !value.canConvertToLong() || value.longValue() < 0) {
      throw new RemoteCallException("invalid non-negative integer: " + field, 200);
    }
    return value.longValue();
  }

  record Reply(JsonNode body, HttpHeaders headers) { }
  @FunctionalInterface interface Sleeper { void sleep(Duration duration) throws InterruptedException; }
}
