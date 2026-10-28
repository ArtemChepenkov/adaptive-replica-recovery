package ru.nsu.replicarecovery.load.web;

import java.util.List;
import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.nsu.replicarecovery.load.core.LoadCommand;
import ru.nsu.replicarecovery.load.metrics.LoadMetricsService;
import ru.nsu.replicarecovery.load.metrics.LoadMetricsService.LoadStatus;
import ru.nsu.replicarecovery.model.ControlSettingsSnapshot;
import ru.nsu.replicarecovery.model.LoadMetricsSnapshot;

@RestController
@RequestMapping("/api/load")
public class LoadController {
  private final LoadMetricsService service;

  public LoadController(LoadMetricsService service) {
    this.service = service;
  }

  @PostMapping("/start")
  public ResponseEntity<LoadStatus> start(@RequestBody LoadStartRequest request) {
    request.validate();
    return ResponseEntity.accepted().body(service.start(new LoadCommand(request.load(), request.seed()),
        new ControlSettingsSnapshot(request.slo(), request.recovery())));
  }

  @PostMapping("/stop")
  public ResponseEntity<LoadStatus> stop() {
    LoadStatus status = service.stop();
    return ResponseEntity.status(status.cleanupComplete() ? HttpStatus.OK : HttpStatus.ACCEPTED).body(status);
  }

  @GetMapping("/status")
  public LoadStatus status() {
    return service.status();
  }

  @GetMapping("/metrics/current")
  public LoadMetricsSnapshot current() {
    return service.current();
  }

  @GetMapping("/metrics")
  public List<LoadMetricsSnapshot> metrics() {
    return service.history();
  }

  @ExceptionHandler(LoadMetricsService.RunConflictException.class)
  public ResponseEntity<Map<String, String>> conflict(RuntimeException exception) {
    return ResponseEntity.status(HttpStatus.CONFLICT).body(Map.of("error", exception.getMessage()));
  }

  @ExceptionHandler({IllegalArgumentException.class, ArithmeticException.class,
      HttpMessageNotReadableException.class})
  public ResponseEntity<Map<String, String>> invalid(RuntimeException exception) {
    return ResponseEntity.badRequest().body(Map.of("error", String.valueOf(exception.getMessage())));
  }

  @ExceptionHandler(RuntimeException.class)
  public ResponseEntity<Map<String, String>> failed(RuntimeException exception) {
    return ResponseEntity.internalServerError().body(Map.of("error", String.valueOf(exception.getMessage())));
  }
}
