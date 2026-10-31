package ru.nsu.replicarecovery.runner.results;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import ru.nsu.replicarecovery.model.ExperimentSpec;
import ru.nsu.replicarecovery.model.ExperimentSpecValidator;
import ru.nsu.replicarecovery.model.ExperimentState;
import ru.nsu.replicarecovery.runner.domain.ExperimentOutcome;
import ru.nsu.replicarecovery.runner.domain.ExperimentRecord;
import ru.nsu.replicarecovery.runner.lab.CleanupResult;
import ru.nsu.replicarecovery.runner.lifecycle.ExperimentStateMachine;
import ru.nsu.replicarecovery.runner.metrics.MonitoringSnapshot;

public final class FileExperimentResultStore implements ExperimentResultStore {
  static final String LEGACY_HEADER = "timestamp,produceP99Ms,errorRate,offeredRps,achievedRps,"
      + "throttleBytesPerSecond,remainingBytes,progressBytesPerSecond,criticalPartitions,safeState,"
      + "brokerCpuPercent,brokerNetworkOutBytesPerSecond,brokerDiskWriteBytesPerSecond\n";
  static final String HEADER = LEGACY_HEADER.stripTrailing() + ",monitoringSnapshot\n";
  private final Path root;
  private final Clock clock;
  private final ObjectMapper json = JsonMapper.builder().findAndAddModules()
      .disable(MapperFeature.ALLOW_COERCION_OF_SCALARS)
      .disable(DeserializationFeature.ACCEPT_FLOAT_AS_INT)
      .enable(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES)
      .enable(DeserializationFeature.FAIL_ON_MISSING_CREATOR_PROPERTIES)
      .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
      .disable(SerializationFeature.WRITE_DURATIONS_AS_TIMESTAMPS).build();
  private final ConcurrentHashMap<String, Object> locks = new ConcurrentHashMap<>();
  private final ExperimentStateMachine states = new ExperimentStateMachine();
  private final SummaryCalculator calculator = new SummaryCalculator();

  public FileExperimentResultStore(Path root, Clock clock) {
    this.root = root.toAbsolutePath().normalize();
    this.clock = clock;
    io(() -> { Files.createDirectories(this.root); return null; });
  }

  @Override
  public ExperimentRecord create(String id, ExperimentSpec spec) {
    return locked(id, () -> {
      new ExperimentSpecValidator().validate(spec);
      Instant now = clock.instant();
      ExperimentRecord record = new ExperimentRecord(1, id, spec, ExperimentState.CREATED,
          ExperimentOutcome.COMPLETED, "COMPLETED", now, now, null, null, null, null);
      return io(() -> {
        Files.createDirectory(directory(id));
        writeAtomic(directory(id).resolve("experiment.json"), record);
        Files.writeString(directory(id).resolve("metrics.csv"), HEADER, StandardCharsets.UTF_8,
            StandardOpenOption.CREATE_NEW);
        Files.createFile(directory(id).resolve("decisions.jsonl"));
        Files.createFile(directory(id).resolve("events.log"));
        event(id, "runner", "INFO", "CREATED");
        return record;
      });
    });
  }

  @Override
  public ExperimentRecord get(String id) {
    return locked(id, () -> readRecord(id));
  }

  @Override
  public List<ExperimentRecord> list() {
    return io(() -> {
      List<ExperimentRecord> records = new ArrayList<>();
      try (var directories = Files.list(root)) {
        for (Path path : directories.sorted().toList()) {
          if (Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS)
              && Files.isRegularFile(path.resolve("experiment.json"), LinkOption.NOFOLLOW_LINKS)) {
            records.add(get(path.getFileName().toString()));
          }
        }
      }
      return List.copyOf(records);
    });
  }

  @Override
  public ExperimentRecord transition(String id, ExperimentState next) {
    return locked(id, () -> {
      ExperimentRecord record = readRecord(id);
      if (next == ExperimentState.COMPLETED || next == ExperimentState.FAILED
          || next == ExperimentState.CANCELLED) {
        throw new IllegalArgumentException("terminal state can only be written by finish after cleanup");
      }
      if (next == ExperimentState.STOPPING && record.state() != ExperimentState.RUNNING
          && record.plannedOutcome() != ExperimentOutcome.CANCELLED) {
        throw new IllegalArgumentException("early stop requires a planned cancellation");
      }
      if (next == ExperimentState.CLEANING_UP && record.state() != ExperimentState.STOPPING
          && record.plannedOutcome() != ExperimentOutcome.FAILED) {
        throw new IllegalArgumentException("early cleanup requires a planned failure");
      }
      states.transition(record.state(), next);
      return saveState(record, next, record.plannedOutcome(), record.reason());
    });
  }

  @Override
  public ExperimentRecord planOutcome(String id, ExperimentOutcome outcome, String reason) {
    return locked(id, () -> {
      ExperimentRecord record = writable(id);
      if (outcome == null || reason == null || reason.isBlank()) {
        throw new IllegalArgumentException("outcome and reason are required");
      }
      if ((record.plannedOutcome() == ExperimentOutcome.FAILED && outcome != ExperimentOutcome.FAILED)
          || (record.plannedOutcome() == ExperimentOutcome.CANCELLED && outcome == ExperimentOutcome.COMPLETED)) {
        throw new IllegalArgumentException("cannot replace a failure or cancellation with success");
      }
      if (outcome == ExperimentOutcome.COMPLETED && record.state() != ExperimentState.RUNNING
          && record.state() != ExperimentState.STOPPING) {
        throw new IllegalArgumentException("successful completion requires the running phase");
      }
      ExperimentState next = outcome == ExperimentOutcome.FAILED
          ? ExperimentState.CLEANING_UP : ExperimentState.STOPPING;
      if (record.state() == ExperimentState.CLEANING_UP) {
        next = ExperimentState.CLEANING_UP;
      }
      if (next != record.state()) {
        states.transition(record.state(), next);
      }
      return saveState(record, next, outcome, reason);
    });
  }

  @Override
  public ExperimentRecord recordTimings(String id, Instant faultAt, Instant recoveryStartedAt) {
    return locked(id, () -> {
      ExperimentRecord record = writable(id);
      checkTiming(record, faultAt);
      checkTiming(record, recoveryStartedAt);
      if ((record.faultAt() != null && !record.faultAt().equals(faultAt))
          || (record.recoveryStartedAt() != null && !record.recoveryStartedAt().equals(recoveryStartedAt))) {
        throw new IllegalArgumentException("actual experiment timings cannot be overwritten");
      }
      if (faultAt != null && recoveryStartedAt != null && recoveryStartedAt.isBefore(faultAt)) {
        throw new IllegalArgumentException("recovery must not start before the fault");
      }
      ExperimentRecord updated = new ExperimentRecord(1, id, record.spec(), record.state(),
          record.plannedOutcome(), record.reason(), record.createdAt(), now(record), faultAt,
          recoveryStartedAt, record.recoveryFinishedAt(), record.measurementEndedAt());
      writeRecord(updated);
      return updated;
    });
  }

  @Override
  public ExperimentRecord recordRecoveryFinished(String id, Instant finishedAt) {
    return locked(id, () -> {
      ExperimentRecord record = writable(id);
      checkTiming(record, finishedAt);
      if (finishedAt == null || record.recoveryStartedAt() == null
          || finishedAt.isBefore(record.recoveryStartedAt())) {
        throw new IllegalArgumentException("recovery completion requires its start time");
      }
      if (record.recoveryFinishedAt() != null && !record.recoveryFinishedAt().equals(finishedAt)) {
        throw new IllegalArgumentException("actual recovery completion cannot be overwritten");
      }
      ExperimentRecord updated = new ExperimentRecord(1, id, record.spec(), record.state(),
          record.plannedOutcome(), record.reason(), record.createdAt(), now(record), record.faultAt(),
          record.recoveryStartedAt(), finishedAt, record.measurementEndedAt());
      writeRecord(updated);
      return updated;
    });
  }

  @Override
  public void appendMetric(String id, ExperimentMetricRow row) {
    locked(id, () -> {
      writable(id);
      Object[] fields = {row.timestamp(), row.produceP99Ms(), row.errorRate(), row.offeredRps(),
          row.achievedRps(), row.throttleBytesPerSecond(), row.remainingBytes(), row.progressBytesPerSecond(),
          row.criticalPartitions(), row.safeState(), row.brokerCpuPercent(),
          row.brokerNetworkOutBytesPerSecond(), row.brokerDiskWriteBytesPerSecond()};
      List<String> values = new ArrayList<>();
      for (Object field : fields) {
        values.add(field == null ? "" : field.toString());
      }
      String header = io(() -> {
        try (var reader = Files.newBufferedReader(directory(id).resolve("metrics.csv"), StandardCharsets.UTF_8)) {
          return reader.readLine();
        }
      });
      if (HEADER.stripTrailing().equals(header)) {
        values.add(row.monitoring() == null ? "" : "\"" + encode(row.monitoring()).replace("\"", "\"\"") + "\"");
      } else if (!LEGACY_HEADER.stripTrailing().equals(header) || row.monitoring() != null) {
        throw new IllegalArgumentException("cannot append monitoring to an invalid or legacy metrics CSV header");
      }
      append(id, "metrics.csv", String.join(",", values));
      return null;
    });
  }

  @Override
  public void appendDecision(String id, Map<String, ?> decision) {
    locked(id, () -> {
      writable(id);
      Map<String, Object> fields = new LinkedHashMap<>(decision);
      fields.put("timestamp", clock.instant());
      append(id, "decisions.jsonl", encode(fields));
      return null;
    });
  }

  @Override
  public void appendEvent(String id, String source, String severity, String message) {
    locked(id, () -> {
      writable(id);
      event(id, source, severity, message);
      return null;
    });
  }

  @Override
  public ExperimentSummary finish(String id, CleanupResult cleanup) {
    return locked(id, () -> {
      ExperimentRecord record = readRecord(id);
      if (record.terminal()) {
        return io(() -> json.readValue(directory(id).resolve("summary.json").toFile(), ExperimentSummary.class));
      }
      if (record.state() != ExperimentState.CLEANING_UP) {
        throw new IllegalStateException("cleanup state is required before finish");
      }
      if (cleanup == null || !cleanup.attempted()) {
        throw new IllegalArgumentException("cleanup must be attempted before finish");
      }
      for (String file : List.of("experiment.json", "metrics.csv", "decisions.jsonl", "events.log")) {
        if (!Files.isRegularFile(directory(id).resolve(file), LinkOption.NOFOLLOW_LINKS)) {
          throw new IllegalStateException("missing result file: " + file);
        }
      }
      Instant finishedAt = now(record);
      List<ExperimentMetricRow> rows;
      boolean dataComplete = true;
      try {
        rows = readMetrics(id);
      } catch (RuntimeException exception) {
        if (record.plannedOutcome() != ExperimentOutcome.FAILED) {
          throw exception;
        }
        dataComplete = false;
        event(id, "runner", "ERROR", "METRICS_UNREADABLE: " + exception.getClass().getSimpleName());
        try {
          rows = readMetrics(id, true);
        } catch (RuntimeException unreadable) {
          rows = List.of();
        }
      }
      ExperimentSummary summary = calculator.calculate(record, rows, finishedAt,
          record.plannedOutcome(), record.reason(), cleanup).withFilesComplete(dataComplete);
      states.transition(record.state(), summary.finalState());
      event(id, "runner", cleanup.success() ? "INFO" : "ERROR", "CLEANUP_ATTEMPTED");
      io(() -> { writeAtomic(directory(id).resolve("summary.json"), summary); return null; });
      ExperimentRecord terminal = new ExperimentRecord(1, id, record.spec(), summary.finalState(),
          record.plannedOutcome(), summary.reason(), record.createdAt(), finishedAt, record.faultAt(),
          record.recoveryStartedAt(), record.recoveryFinishedAt(), record.measurementEndedAt());
      // The final marker is last: a crash between these writes is recovered as incomplete.
      writeRecord(terminal);
      return summary;
    });
  }

  @Override
  public List<ExperimentMetricRow> metrics(String id) {
    return locked(id, () -> { readRecord(id); return List.copyOf(readMetrics(id)); });
  }

  @Override
  public List<Map<String, Object>> events(String id) {
    return readJournal(id, "events.log");
  }

  @Override
  public List<Map<String, Object>> decisions(String id) {
    return readJournal(id, "decisions.jsonl");
  }

  @Override
  public ExperimentSummary summary(String id) {
    return locked(id, () -> {
      ExperimentRecord record = readRecord(id);
      if (!record.terminal()) throw new IllegalStateException("summary is not final yet");
      return io(() -> json.readValue(directory(id).resolve("summary.json").toFile(), ExperimentSummary.class));
    });
  }

  private List<Map<String, Object>> readJournal(String id, String filename) {
    return locked(id, () -> {
      readRecord(id);
      return io(() -> {
        List<Map<String, Object>> entries = new ArrayList<>();
        for (String line : Files.readAllLines(directory(id).resolve(filename), StandardCharsets.UTF_8)) {
          entries.add(json.readValue(line, new com.fasterxml.jackson.core.type.TypeReference<Map<String, Object>>() { }));
        }
        return List.copyOf(entries);
      });
    });
  }

  private ExperimentRecord saveState(ExperimentRecord record, ExperimentState next,
      ExperimentOutcome outcome, String reason) {
    Instant timestamp = now(record);
    Instant measurementEnd = record.measurementEndedAt();
    if (measurementEnd == null && (next == ExperimentState.STOPPING || next == ExperimentState.CLEANING_UP)) {
      measurementEnd = timestamp;
      if (reason.equals("RUNNER_RESTART")) {
        // The crash time is unknown: never extrapolate the final sample through downtime.
        try {
          measurementEnd = readMetrics(record.id(), true).stream().map(ExperimentMetricRow::timestamp)
              .filter(value -> !value.isBefore(record.createdAt()) && !value.isAfter(timestamp))
              .max(Comparator.naturalOrder()).orElse(record.createdAt());
        } catch (RuntimeException exception) {
          measurementEnd = record.createdAt();
        }
      }
    }
    ExperimentRecord updated = new ExperimentRecord(1, record.id(), record.spec(), next, outcome,
        reason, record.createdAt(), timestamp, record.faultAt(), record.recoveryStartedAt(),
        record.recoveryFinishedAt(), measurementEnd);
    writeRecord(updated);
    event(record.id(), "runner", "INFO", "STATE_" + next);
    return updated;
  }

  private ExperimentRecord writable(String id) {
    ExperimentRecord record = readRecord(id);
    if (record.terminal()) {
      throw new IllegalStateException("experiment is already terminal: " + id);
    }
    return record;
  }

  private ExperimentRecord readRecord(String id) {
    return io(() -> {
      ExperimentRecord record = json.readValue(directory(id).resolve("experiment.json").toFile(), ExperimentRecord.class);
      if (record.schemaVersion() != 1 || !id.equals(record.id()) || record.state() == null
          || record.plannedOutcome() == null || record.createdAt() == null || record.updatedAt() == null
          || record.updatedAt().isBefore(record.createdAt())) {
        throw new IllegalArgumentException("invalid persisted experiment: " + id);
      }
      new ExperimentSpecValidator().validate(record.spec());
      if (record.terminal()) {
        ExperimentSummary summary = json.readValue(directory(id).resolve("summary.json").toFile(), ExperimentSummary.class);
        if (summary.schemaVersion() != 1 || !id.equals(summary.experimentId())
            || summary.finalState() != record.state() || summary.cleanup() == null || !summary.cleanup().attempted()) {
          throw new IllegalArgumentException("inconsistent final result: " + id);
        }
      }
      return record;
    });
  }

  private List<ExperimentMetricRow> readMetrics(String id) {
    return readMetrics(id, false);
  }

  private List<ExperimentMetricRow> readMetrics(String id, boolean tolerateTornTail) {
    return io(() -> {
      List<String> lines = Files.readAllLines(directory(id).resolve("metrics.csv"), StandardCharsets.UTF_8);
      if (lines.isEmpty() || !(lines.get(0).equals(HEADER.stripTrailing())
          || lines.get(0).equals(LEGACY_HEADER.stripTrailing()))) {
        throw new IllegalArgumentException("invalid metrics CSV header");
      }
      int columns = lines.get(0).equals(HEADER.stripTrailing()) ? 14 : 13;
      List<ExperimentMetricRow> rows = new ArrayList<>();
      for (String line : lines.subList(1, lines.size())) {
        try {
          rows.add(parseMetric(line, columns));
        } catch (RuntimeException exception) {
          if (tolerateTornTail) {
            break;
          }
          throw exception;
        }
      }
      return rows;
    });
  }

  private ExperimentMetricRow parseMetric(String line, int columns) {
    // The first 13 fields are validated numbers/booleans/ISO timestamps and cannot contain commas.
    // Only the final monitoring field is quoted CSV containing compact JSON.
    String[] values = line.split(",", columns);
    if (values.length != columns || !(values[9].equals("true") || values[9].equals("false"))) {
      throw new IllegalArgumentException("invalid metrics CSV row");
    }
    return new ExperimentMetricRow(Instant.parse(values[0]), nullable(values[1]),
        Double.parseDouble(values[2]), Double.parseDouble(values[3]), Double.parseDouble(values[4]),
        Long.parseLong(values[5]), Long.parseLong(values[6]), Long.parseLong(values[7]),
        Integer.parseInt(values[8]), Boolean.parseBoolean(values[9]), nullable(values[10]),
        nullable(values[11]), nullable(values[12]), columns == 14 ? parseMonitoring(values[13]) : null);
  }

  private MonitoringSnapshot parseMonitoring(String field) {
    if (field.isEmpty()) return null;
    if (field.length() < 2 || field.charAt(0) != '"' || field.charAt(field.length() - 1) != '"') {
      throw new IllegalArgumentException("monitoring JSON must be a quoted CSV field");
    }
    StringBuilder decoded = new StringBuilder();
    for (int index = 1; index < field.length() - 1; index++) {
      char value = field.charAt(index);
      if (value == '"') {
        if (index + 1 >= field.length() - 1 || field.charAt(++index) != '"') {
          throw new IllegalArgumentException("invalid quote in monitoring CSV field");
        }
      }
      decoded.append(value);
    }
    return io(() -> json.readValue(decoded.toString(), MonitoringSnapshot.class));
  }

  private void event(String id, String source, String severity, String message) {
    append(id, "events.log", encode(Map.of("timestamp", clock.instant(), "source", source,
        "severity", severity, "message", message)));
  }

  private void append(String id, String file, String line) {
    io(() -> {
      Files.writeString(directory(id).resolve(file), line + "\n", StandardCharsets.UTF_8,
          StandardOpenOption.APPEND);
      return null;
    });
  }

  private void writeRecord(ExperimentRecord record) {
    io(() -> { writeAtomic(directory(record.id()).resolve("experiment.json"), record); return null; });
  }

  private void writeAtomic(Path target, Object value) throws IOException {
    Path temporary = target.resolveSibling(target.getFileName() + ".tmp");
    try {
      Files.writeString(temporary, json.writeValueAsString(value), StandardCharsets.UTF_8);
      Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    } finally {
      Files.deleteIfExists(temporary);
    }
  }

  private String encode(Object value) {
    return io(() -> json.writeValueAsString(value));
  }

  private Path directory(String id) {
    if (id == null || !id.matches("[a-zA-Z0-9][a-zA-Z0-9_-]{0,127}")) {
      throw new IllegalArgumentException("invalid experiment id");
    }
    Path path = root.resolve(id);
    if (Files.isSymbolicLink(path)) {
      throw new IllegalArgumentException("experiment directory must not be a symbolic link");
    }
    return path;
  }

  private <T> T locked(String id, Supplier<T> action) {
    directory(id);
    synchronized (locks.computeIfAbsent(id, ignored -> new Object())) {
      return action.get();
    }
  }

  private Instant now(ExperimentRecord record) {
    Instant timestamp = clock.instant();
    if (timestamp.isBefore(record.updatedAt())) {
      throw new IllegalArgumentException("clock moved before the last recorded state");
    }
    return timestamp;
  }

  private void checkTiming(ExperimentRecord record, Instant timestamp) {
    if (timestamp != null && (timestamp.isBefore(record.createdAt()) || timestamp.isAfter(clock.instant()))) {
      throw new IllegalArgumentException("timing must be within the experiment lifetime");
    }
  }

  private static Double nullable(String value) {
    return value.isEmpty() ? null : Double.valueOf(value);
  }

  private static <T> T io(IoSupplier<T> action) {
    try {
      return action.get();
    } catch (IOException exception) {
      throw new UncheckedIOException(exception);
    }
  }

  private interface IoSupplier<T> { T get() throws IOException; }
}
