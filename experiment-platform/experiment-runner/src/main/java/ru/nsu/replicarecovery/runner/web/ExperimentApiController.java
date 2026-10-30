package ru.nsu.replicarecovery.runner.web;

import com.fasterxml.jackson.databind.JsonMappingException;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.nsu.replicarecovery.runner.config.ExperimentSpecLoader;
import ru.nsu.replicarecovery.runner.config.ScenarioCatalog;
import ru.nsu.replicarecovery.runner.domain.ExperimentRecord;
import ru.nsu.replicarecovery.runner.lab.CleanupResult;
import ru.nsu.replicarecovery.runner.lifecycle.ExperimentCoordinator;
import ru.nsu.replicarecovery.runner.results.ExperimentMetricRow;

@RestController
@RequestMapping("/api")
public class ExperimentApiController {
  private static final Logger LOG = LoggerFactory.getLogger(ExperimentApiController.class);
  private static final Pattern VALID_ID = Pattern.compile("[a-zA-Z0-9][a-zA-Z0-9_-]{0,127}");
  private static final Pattern VALIDATION_FIELD = Pattern.compile("^([a-zA-Z][a-zA-Z0-9_.]*) (?:must|is required)");
  private final ExperimentCoordinator coordinator;
  private final ExperimentSpecLoader loader;
  private final ScenarioCatalog scenarios;

  public ExperimentApiController(ExperimentCoordinator coordinator, ExperimentSpecLoader loader,
      ScenarioCatalog scenarios) {
    this.coordinator = coordinator;
    this.loader = loader;
    this.scenarios = scenarios;
  }

  @PostMapping(value = "/experiments", consumes = {
      "application/x-yaml", "application/yaml", "text/yaml", "text/plain", MediaType.APPLICATION_JSON_VALUE})
  public ResponseEntity<Map<String, Object>> start(@RequestBody String document) {
    ExperimentRecord record = coordinator.start(loader.load(document));
    return ResponseEntity.accepted().body(Map.of("id", record.id(), "state", record.state()));
  }

  @GetMapping("/experiments")
  public List<ExperimentRecord> list() {
    return coordinator.list();
  }

  @GetMapping("/experiments/{id}")
  public ExperimentRecord get(@PathVariable String id) {
    validateId(id);
    return coordinator.get(id);
  }

  @PostMapping("/experiments/{id}/stop")
  public ResponseEntity<ExperimentRecord> stop(@PathVariable String id) {
    validateId(id);
    ExperimentRecord record = coordinator.stop(id);
    return ResponseEntity.status(record.terminal() ? HttpStatus.OK : HttpStatus.ACCEPTED).body(record);
  }

  @GetMapping("/experiments/{id}/metrics")
  public List<ExperimentMetricRow> metrics(@PathVariable String id) {
    validateId(id);
    return coordinator.metrics(id);
  }

  @GetMapping("/experiments/{id}/events")
  public Map<String, Object> events(@PathVariable String id) {
    validateId(id);
    return Map.of("events", coordinator.events(id), "decisions", coordinator.decisions(id));
  }

  @GetMapping("/profiles")
  public List<ScenarioCatalog.Profile> profiles() {
    return scenarios.profiles();
  }

  @PostMapping("/lab/reset")
  public ResponseEntity<?> reset() {
    CleanupResult result = coordinator.reset();
    if (result.success()) {
      return ResponseEntity.ok(result);
    }
    ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.SERVICE_UNAVAILABLE,
        "Lab cleanup could not be verified");
    problem.setProperty("cleanup", result);
    return problem(HttpStatus.SERVICE_UNAVAILABLE, problem);
  }

  @ExceptionHandler(ExperimentCoordinator.BusyException.class)
  public ResponseEntity<ProblemDetail> conflict() {
    return problem(HttpStatus.CONFLICT, ProblemDetail.forStatusAndDetail(HttpStatus.CONFLICT,
        "An experiment or lab cleanup is already active; wait for verified cleanup or reset the lab"));
  }

  @ExceptionHandler(ExperimentCoordinator.UnknownExperimentException.class)
  public ResponseEntity<ProblemDetail> notFound() {
    return problem(HttpStatus.NOT_FOUND, ProblemDetail.forStatusAndDetail(HttpStatus.NOT_FOUND,
        "Experiment not found"));
  }

  @ExceptionHandler({IllegalArgumentException.class, HttpMessageNotReadableException.class})
  public ResponseEntity<ProblemDetail> invalid(RuntimeException exception) {
    String detail = exception.getMessage() == null ? "Invalid experiment configuration" : exception.getMessage();
    ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, detail);
    problem.setProperty("field", field(exception));
    return problem(HttpStatus.BAD_REQUEST, problem);
  }

  @ExceptionHandler(RuntimeException.class)
  public ResponseEntity<ProblemDetail> failed(RuntimeException exception) {
    LOG.error("Runner API operation failed", exception);
    return problem(HttpStatus.INTERNAL_SERVER_ERROR, ProblemDetail.forStatusAndDetail(
        HttpStatus.INTERNAL_SERVER_ERROR, "The experiment operation failed; inspect the Runner logs"));
  }

  private static void validateId(String id) {
    if (id == null || !VALID_ID.matcher(id).matches()) {
      throw new IllegalArgumentException("experimentId must contain only letters, digits, underscores and hyphens");
    }
  }

  private static ResponseEntity<ProblemDetail> problem(HttpStatus status, ProblemDetail detail) {
    return ResponseEntity.status(status).contentType(MediaType.APPLICATION_PROBLEM_JSON).body(detail);
  }

  private static String field(Throwable error) {
    for (Throwable cause = error; cause != null; cause = cause.getCause()) {
      if (cause instanceof JsonMappingException mapping && !mapping.getPath().isEmpty()) {
        StringBuilder field = new StringBuilder();
        for (JsonMappingException.Reference reference : mapping.getPath()) {
          if (reference.getFieldName() != null) {
            if (!field.isEmpty()) {
              field.append('.');
            }
            field.append(reference.getFieldName());
          } else if (reference.getIndex() >= 0) {
            field.append('[').append(reference.getIndex()).append(']');
          }
        }
        if (!field.isEmpty()) {
          return field.toString();
        }
      }
      if (cause.getMessage() != null) {
        var match = VALIDATION_FIELD.matcher(cause.getMessage());
        if (match.find()) {
          return match.group(1);
        }
      }
    }
    return "configuration";
  }
}
