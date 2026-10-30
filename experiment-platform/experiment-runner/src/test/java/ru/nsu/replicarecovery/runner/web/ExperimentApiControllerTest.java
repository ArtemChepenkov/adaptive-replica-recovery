package ru.nsu.replicarecovery.runner.web;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.time.Instant;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.http.MediaType;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import ru.nsu.replicarecovery.model.ExperimentSpec;
import ru.nsu.replicarecovery.model.ExperimentState;
import ru.nsu.replicarecovery.runner.config.ExperimentSpecLoader;
import ru.nsu.replicarecovery.runner.config.ScenarioCatalog;
import ru.nsu.replicarecovery.runner.domain.ExperimentOutcome;
import ru.nsu.replicarecovery.runner.domain.ExperimentRecord;
import ru.nsu.replicarecovery.runner.lab.CleanupResult;
import ru.nsu.replicarecovery.runner.lifecycle.ExperimentCoordinator;
import ru.nsu.replicarecovery.runner.results.ExperimentMetricRow;

class ExperimentApiControllerTest {
  private static final String YAML = """
      schemaVersion: 1
      name: api-test
      timeout: 30s
      slo: {produceP99Ms: 100, maxErrorRate: 0.01}
      load: {mode: CONSTANT, messageSizeBytes: 1024, schedule: [{duration: 20s, requestsPerSecond: 100}]}
      recovery: {throttlePolicy: AIMD, initialThrottleMiBps: 20, minThrottleMiBps: 1, maxThrottleMiBps: 100}
      resources: {profile: normal}
      fault: {action: STOP_BROKER, target: broker-2, at: 1s}
      """;
  private final ExperimentCoordinator coordinator = mock(ExperimentCoordinator.class);
  private final ScenarioCatalog catalog = mock(ScenarioCatalog.class);
  private final ExperimentSpecLoader loader = new ExperimentSpecLoader();
  private final ObjectMapper json = new ObjectMapper().findAndRegisterModules()
      .addMixIn(org.springframework.http.ProblemDetail.class,
          org.springframework.http.converter.json.ProblemDetailJacksonMixin.class)
      .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
      .disable(SerializationFeature.WRITE_DURATIONS_AS_TIMESTAMPS);
  private MockMvc mvc;

  @BeforeEach
  void setUp() {
    mvc = MockMvcBuilders.standaloneSetup(new ExperimentApiController(coordinator, loader, catalog))
        .setMessageConverters(new StringHttpMessageConverter(StandardCharsets.UTF_8),
            new MappingJackson2HttpMessageConverter(json)).build();
  }

  @Test
  void startsNormalizedYamlAndReturnsOnlyTheAcceptedIdentityAndState() throws Exception {
    when(coordinator.start(any())).thenReturn(record(ExperimentState.CREATED));
    mvc.perform(post("/api/experiments").contentType("application/x-yaml").content(YAML))
        .andExpect(status().isAccepted()).andExpect(jsonPath("$.id").value("run-001"))
        .andExpect(jsonPath("$.state").value("CREATED"));
    ArgumentCaptor<ExperimentSpec> specification = ArgumentCaptor.forClass(ExperimentSpec.class);
    verify(coordinator).start(specification.capture());
    assertThat(specification.getValue().seed()).isEqualTo(20260910);
    assertThat(specification.getValue().recovery().safeFallbackThrottleMiBps()).isEqualTo(20);
  }

  @Test
  void acceptsJsonThroughTheSameConfigurationLoader() throws Exception {
    when(coordinator.start(any())).thenReturn(record(ExperimentState.CREATED));
    mvc.perform(post("/api/experiments").contentType(MediaType.APPLICATION_JSON)
        .content(json.writeValueAsBytes(loader.load(YAML))))
        .andExpect(status().isAccepted());
  }

  @Test
  void describesInvalidSchemaAndMalformedDocumentsAsProblems() throws Exception {
    mvc.perform(post("/api/experiments").contentType("application/x-yaml")
        .content(YAML.replace("schemaVersion: 1", "schemaVersion: 2")))
        .andExpect(status().isBadRequest())
        .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
        .andExpect(jsonPath("$.status").value(400))
        .andExpect(jsonPath("$.field").value("schemaVersion"))
        .andExpect(jsonPath("$.detail").isString());
    mvc.perform(post("/api/experiments").contentType("application/json").content("{broken"))
        .andExpect(status().isBadRequest()).andExpect(jsonPath("$.field").isString());
    mvc.perform(post("/api/experiments").contentType("application/x-yaml").content(""))
        .andExpect(status().isBadRequest());
  }

  @Test
  void reportsConflictNotFoundAndInvalidIdWithProblemDetails() throws Exception {
    when(coordinator.start(any())).thenThrow(mock(ExperimentCoordinator.BusyException.class));
    mvc.perform(post("/api/experiments").contentType("application/x-yaml").content(YAML))
        .andExpect(status().isConflict()).andExpect(jsonPath("$.status").value(409));
    when(coordinator.get("unknown")).thenThrow(mock(ExperimentCoordinator.UnknownExperimentException.class));
    mvc.perform(get("/api/experiments/unknown"))
        .andExpect(status().isNotFound()).andExpect(jsonPath("$.status").value(404));
    mvc.perform(get("/api/experiments/bad.id"))
        .andExpect(status().isBadRequest());
  }

  @Test
  void listsExperimentsAndReturnsDurableStatus() throws Exception {
    when(coordinator.list()).thenReturn(List.of(record(ExperimentState.RUNNING)));
    when(coordinator.get("run-001")).thenReturn(record(ExperimentState.RUNNING));
    mvc.perform(get("/api/experiments")).andExpect(status().isOk())
        .andExpect(jsonPath("$[0].id").value("run-001"));
    mvc.perform(get("/api/experiments/run-001")).andExpect(status().isOk())
        .andExpect(jsonPath("$.state").value("RUNNING"))
        .andExpect(jsonPath("$.spec.slo.produceP99Ms").value(100));
  }

  @Test
  void stopReturnsAcceptedUntilTheExperimentBecomesTerminal() throws Exception {
    when(coordinator.stop("run-001")).thenReturn(record(ExperimentState.STOPPING));
    mvc.perform(post("/api/experiments/run-001/stop"))
        .andExpect(status().isAccepted()).andExpect(jsonPath("$.state").value("STOPPING"));
    when(coordinator.stop("run-001")).thenReturn(record(ExperimentState.CANCELLED));
    mvc.perform(post("/api/experiments/run-001/stop"))
        .andExpect(status().isOk()).andExpect(jsonPath("$.state").value("CANCELLED"));
  }

  @Test
  void returnsMetricRowsAndKeepsEventsAndDecisionsSeparate() throws Exception {
    var row = new ExperimentMetricRow(Instant.parse("2026-10-29T10:00:00Z"), 12.0,
        0.01, 100, 99, 20, 100, 1, 3, false, null, null, null);
    when(coordinator.metrics("run-001")).thenReturn(List.of(row));
    when(coordinator.events("run-001")).thenReturn(List.of(Map.of("message", "RECOVERY_STARTED")));
    when(coordinator.decisions("run-001")).thenReturn(List.of(Map.of("reason", "SLO_HEALTHY")));
    mvc.perform(get("/api/experiments/run-001/metrics"))
        .andExpect(status().isOk()).andExpect(jsonPath("$[0].produceP99Ms").value(12.0))
        .andExpect(jsonPath("$[0].brokerCpuPercent").isEmpty());
    mvc.perform(get("/api/experiments/run-001/events"))
        .andExpect(status().isOk()).andExpect(jsonPath("$.events[0].message").value("RECOVERY_STARTED"))
        .andExpect(jsonPath("$.decisions[0].reason").value("SLO_HEALTHY"));
  }

  @Test
  void listsScenarioFilesAndReportsFailedResetWithoutPretendingSuccess() throws Exception {
    when(catalog.profiles()).thenReturn(List.of(new ScenarioCatalog.Profile("smoke.yaml", YAML)));
    mvc.perform(get("/api/profiles")).andExpect(status().isOk())
        .andExpect(jsonPath("$[0].name").value("smoke.yaml"))
        .andExpect(jsonPath("$[0].yaml").value(YAML));
    when(coordinator.reset()).thenReturn(CleanupResult.ok());
    mvc.perform(post("/api/lab/reset")).andExpect(status().isOk())
        .andExpect(jsonPath("$.success").value(true));
    when(coordinator.reset()).thenReturn(new CleanupResult(true, false, List.of("reset failed")));
    mvc.perform(post("/api/lab/reset")).andExpect(status().isServiceUnavailable())
        .andExpect(jsonPath("$.cleanup.success").value(false));
  }

  @Test
  void unexpectedFailuresDoNotExposeFilesystemOrStackDetails() throws Exception {
    when(coordinator.list()).thenThrow(new IllegalStateException("private filesystem path"));
    String body = mvc.perform(get("/api/experiments")).andExpect(status().isInternalServerError())
        .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
        .andReturn().getResponse().getContentAsString();
    assertThat(body).doesNotContain("private filesystem path");
  }

  private ExperimentRecord record(ExperimentState state) {
    Instant time = Instant.parse("2026-10-29T10:00:00Z");
    return new ExperimentRecord(1, "run-001", loader.load(YAML), state,
        state == ExperimentState.CANCELLED ? ExperimentOutcome.CANCELLED : ExperimentOutcome.COMPLETED,
        "TEST", time, time, null, null, null, null);
  }
}
