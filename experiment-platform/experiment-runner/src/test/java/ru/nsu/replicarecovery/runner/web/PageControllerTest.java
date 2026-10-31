package ru.nsu.replicarecovery.runner.web;

import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.model;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.redirectedUrl;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.view;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

import java.nio.file.Path;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import ru.nsu.replicarecovery.model.ExperimentState;
import ru.nsu.replicarecovery.runner.domain.ExperimentOutcome;
import ru.nsu.replicarecovery.runner.domain.ExperimentRecord;
import ru.nsu.replicarecovery.runner.lifecycle.ExperimentCoordinator;

/** Uses the real Thymeleaf resolver and static resource handling, not just view names. */
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("test-lab")
class PageControllerTest {
  @TempDir static Path results;
  @Autowired MockMvc mvc;
  @MockitoBean ExperimentCoordinator coordinator;

  @DynamicPropertySource
  static void configuration(DynamicPropertyRegistry properties) {
    properties.add("runner.results-directory", () -> results.toString());
  }

  @Test
  void rendersStartFormAndExplicitSimulationMode() throws Exception {
    mvc.perform(get("/"))
        .andExpect(status().isOk()).andExpect(view().name("index"))
        .andExpect(model().attribute("simulated", true))
        .andExpect(content().string(containsString("Запустить эксперимент")))
        .andExpect(content().string(containsString("data-simulated=\"true\"")))
        .andExpect(content().string(containsString("/js/start-experiment.js")));
  }

  @Test
  void rendersLivePageForAnExistingExperiment() throws Exception {
    when(coordinator.get("run-1")).thenReturn(record(ExperimentState.RUNNING));
    mvc.perform(get("/experiments/run-1"))
        .andExpect(status().isOk()).andExpect(view().name("active"))
        .andExpect(model().attribute("experimentId", "run-1"))
        .andExpect(content().string(containsString("Остановить")))
        .andExpect(content().string(containsString("data-experiment-id=\"run-1\"")))
        .andExpect(content().string(containsString("/vendor/chart.umd.js")));
  }

  @Test
  void rendersResultsOnlyAfterFinalizationAndRedirectsPendingRun() throws Exception {
    when(coordinator.get("run-1")).thenReturn(record(ExperimentState.RUNNING));
    mvc.perform(get("/experiments/run-1/results"))
        .andExpect(status().is3xxRedirection()).andExpect(redirectedUrl("/experiments/run-1"));
    when(coordinator.get("run-1")).thenReturn(record(ExperimentState.COMPLETED));
    mvc.perform(get("/experiments/run-1/results"))
        .andExpect(status().isOk()).andExpect(view().name("results"))
        .andExpect(content().string(containsString("Время до безопасного состояния")))
        .andExpect(content().string(containsString("/js/results-experiment.js")));
  }

  @Test
  void reportsUnknownAndInvalidIdsWithoutRenderingAnExperiment() throws Exception {
    when(coordinator.get("missing")).thenThrow(new ExperimentCoordinator.UnknownExperimentException("missing"));
    mvc.perform(get("/experiments/missing")).andExpect(status().isNotFound());
    mvc.perform(get("/experiments/bad.id")).andExpect(status().isBadRequest());
  }

  @Test
  void servesLocalAssetsWithoutRuntimeCdnDependencies() throws Exception {
    for (String path : new String[] {"/css/app.css", "/js/ui.js", "/js/start-experiment.js",
        "/js/active-experiment.js", "/js/results-experiment.js", "/vendor/chart.umd.js", "/vendor/js-yaml.min.js"}) {
      mvc.perform(get(path)).andExpect(status().isOk());
    }
    mvc.perform(get("/"))
        .andExpect(content().string(not(containsString("src=\"http"))));
  }

  private ExperimentRecord record(ExperimentState state) {
    Instant time = Instant.parse("2026-10-31T10:00:00Z");
    return new ExperimentRecord(1, "run-1", null, state, ExperimentOutcome.COMPLETED,
        "TEST", time, time, null, null, null, null);
  }
}
