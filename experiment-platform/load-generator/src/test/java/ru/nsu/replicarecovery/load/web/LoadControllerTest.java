package ru.nsu.replicarecovery.load.web;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.http.MediaType;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import ru.nsu.replicarecovery.load.core.MessagePublisher;
import ru.nsu.replicarecovery.load.metrics.LoadMetricsService;
import ru.nsu.replicarecovery.model.LoadMode;
import ru.nsu.replicarecovery.model.LoadSegment;
import ru.nsu.replicarecovery.model.LoadSpec;
import ru.nsu.replicarecovery.model.RecoverySpec;
import ru.nsu.replicarecovery.model.SloSpec;

class LoadControllerTest {
  @TempDir Path directory;
  private final CountDownLatch sent = new CountDownLatch(1);
  private final ObjectMapper mapper = new ObjectMapper().findAndRegisterModules()
      .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
      .disable(SerializationFeature.WRITE_DURATIONS_AS_TIMESTAMPS);
  private LoadMetricsService service;
  private MockMvc mvc;

  @BeforeEach
  void setUp() {
    service = new LoadMetricsService(() -> new MessagePublisher() {
      @Override
      public void send(long seed, long sequence, byte[] payload, Consumer<Exception> callback) {
        callback.accept(null);
        sent.countDown();
      }

      @Override
      public void close(Duration timeout) { }
    }, directory, Clock.systemUTC());
    mvc = MockMvcBuilders.standaloneSetup(new LoadController(service))
        .setMessageConverters(new MappingJackson2HttpMessageConverter(mapper)).build();
  }

  @AfterEach
  void close() {
    service.close();
  }

  @Test
  void startsMeasuresAndRejectsConcurrentStart() throws Exception {
    mvc.perform(post("/api/load/start").contentType(MediaType.APPLICATION_JSON)
        .content(mapper.writeValueAsBytes(request(100))))
        .andExpect(status().isAccepted()).andExpect(jsonPath("$.state").value("RUNNING"));
    assertThat(sent.await(2, TimeUnit.SECONDS)).isTrue();
    mvc.perform(get("/api/load/metrics/current"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.windowStart").exists())
        .andExpect(jsonPath("$.windowEnd").exists())
        .andExpect(jsonPath("$.p99Ms").isNumber())
        .andExpect(jsonPath("$.errorRate").isNumber())
        .andExpect(jsonPath("$.controlSettings.slo.produceP99Ms").value(100.0))
        .andExpect(jsonPath("$.controlSettings.recovery.additiveStepMiBps").value(5));
    mvc.perform(post("/api/load/start").contentType(MediaType.APPLICATION_JSON)
        .content(mapper.writeValueAsBytes(request(200)))).andExpect(status().isConflict());
    mvc.perform(get("/api/load/metrics/current"))
        .andExpect(jsonPath("$.controlSettings.slo.produceP99Ms").value(100.0));
    mvc.perform(get("/api/load/metrics")).andExpect(status().isOk()).andExpect(jsonPath("$").isArray());
  }

  @Test
  void stopIsIdempotentAndNewRunGetsItsOwnSettingsAndLog() throws Exception {
    mvc.perform(post("/api/load/start").contentType(MediaType.APPLICATION_JSON)
        .content(mapper.writeValueAsBytes(request(100)))).andExpect(status().isAccepted());
    assertThat(sent.await(2, TimeUnit.SECONDS)).isTrue();
    mvc.perform(post("/api/load/stop")).andExpect(status().isOk())
        .andExpect(jsonPath("$.state").value("STOPPED"));
    String stopped = mvc.perform(post("/api/load/stop")).andExpect(status().isOk())
        .andReturn().getResponse().getContentAsString();
    assertThat(mapper.readTree(stopped).get("state").asText()).isEqualTo("STOPPED");
    mvc.perform(post("/api/load/start").contentType(MediaType.APPLICATION_JSON)
        .content(mapper.writeValueAsBytes(request(200)))).andExpect(status().isAccepted());
    mvc.perform(get("/api/load/metrics/current"))
        .andExpect(jsonPath("$.controlSettings.slo.produceP99Ms").value(200.0));
    mvc.perform(post("/api/load/stop")).andExpect(status().isOk());
    try (var files = Files.walk(directory)) {
      assertThat(files.filter(path -> path.getFileName().toString().equals("requests.csv"))).hasSize(2);
    }
  }

  @Test
  void recomputesP99IndependentlyFromTheClosedCsv() throws Exception {
    LoadStartRequest base = request(100);
    LoadStartRequest shortRun = new LoadStartRequest(new LoadSpec(LoadMode.CONSTANT, 1024,
        List.of(new LoadSegment(Duration.ofMillis(250), 400))), base.slo(), base.recovery(), base.seed());
    mvc.perform(post("/api/load/start").contentType(MediaType.APPLICATION_JSON)
        .content(mapper.writeValueAsBytes(shortRun))).andExpect(status().isAccepted());
    var completed = service.completion().get(3, TimeUnit.SECONDS);
    assertThat(completed.state()).isEqualTo("COMPLETED");
    assertThat(completed.cleanupComplete()).isTrue();
    var snapshot = mapper.readTree(mvc.perform(get("/api/load/metrics/current"))
        .andExpect(status().isOk()).andReturn().getResponse().getContentAsString());
    long start = Instant.parse(snapshot.get("windowStart").asText()).toEpochMilli();
    long end = Instant.parse(snapshot.get("windowEnd").asText()).toEpochMilli();
    List<Long> latencies;
    try (var rows = Files.lines(Path.of(completed.rawLogPath()))) {
      latencies = rows.skip(1).map(row -> row.split(",", -1))
          .filter(row -> Long.parseLong(row[0]) >= start && Long.parseLong(row[0]) <= end)
          .filter(row -> Boolean.parseBoolean(row[3]))
          .map(row -> Long.parseLong(row[2])).sorted().toList();
    }
    assertThat(latencies).hasSize(100);
    double exactP99 = latencies.get((int) Math.ceil(latencies.size() * 0.99) - 1) / 1000.0;
    assertThat(Math.abs(snapshot.get("p99Ms").asDouble() - exactP99)).isLessThanOrEqualTo(1.0);
    assertThat(snapshot.get("requestCount").asLong()).isEqualTo(100);
  }

  @Test
  void rejectsInvalidRequestsAndReportsEmptyWindowHonestly() throws Exception {
    mvc.perform(get("/api/load/metrics/current")).andExpect(status().isOk())
        .andExpect(jsonPath("$.requestCount").value(0))
        .andExpect(jsonPath("$.p99Ms").isEmpty());
    mvc.perform(post("/api/load/start").contentType(MediaType.APPLICATION_JSON).content("{}"))
        .andExpect(status().isBadRequest());
    mvc.perform(post("/api/load/start").contentType(MediaType.APPLICATION_JSON).content("{broken"))
        .andExpect(status().isBadRequest());
    mvc.perform(post("/api/load/start").contentType(MediaType.APPLICATION_JSON)
        .content(mapper.writeValueAsBytes(request(-1)))).andExpect(status().isBadRequest());
    mvc.perform(post("/api/load/start").contentType(MediaType.APPLICATION_JSON)
        .content(mapper.writeValueAsBytes(request(Double.POSITIVE_INFINITY))))
        .andExpect(status().isBadRequest());
    mvc.perform(get("/api/load/status")).andExpect(jsonPath("$.state").value("IDLE"));
  }

  public static LoadStartRequest request(double sloMs) {
    return new LoadStartRequest(new LoadSpec(LoadMode.CONSTANT, 1024,
        List.of(new LoadSegment(Duration.ofSeconds(30), 1))),
        new SloSpec(sloMs, 0.01, Duration.ofMinutes(2), 1000),
        new RecoverySpec("AIMD", true, true, 20, 1, 100, 5, 20, 0.7,
            Duration.ofSeconds(10), Duration.ofSeconds(60), Duration.ofSeconds(30),
            10, 0.85, 2, 100, Duration.ofSeconds(20), 5, 1, 3, 3, 3), 42);
  }
}
