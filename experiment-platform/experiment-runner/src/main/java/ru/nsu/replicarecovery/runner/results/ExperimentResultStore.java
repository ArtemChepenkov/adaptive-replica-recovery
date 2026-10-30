package ru.nsu.replicarecovery.runner.results;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import ru.nsu.replicarecovery.model.ExperimentSpec;
import ru.nsu.replicarecovery.model.ExperimentState;
import ru.nsu.replicarecovery.runner.domain.ExperimentOutcome;
import ru.nsu.replicarecovery.runner.domain.ExperimentRecord;
import ru.nsu.replicarecovery.runner.lab.CleanupResult;

public interface ExperimentResultStore {
  ExperimentRecord create(String id, ExperimentSpec spec);
  default ExperimentRecord create(ExperimentSpec spec) {
    return create(UUID.randomUUID().toString(), spec);
  }
  ExperimentRecord get(String id);
  List<ExperimentRecord> list();
  ExperimentRecord transition(String id, ExperimentState next);
  ExperimentRecord planOutcome(String id, ExperimentOutcome outcome, String reason);
  ExperimentRecord recordTimings(String id, Instant faultAt, Instant recoveryStartedAt);
  ExperimentRecord recordRecoveryFinished(String id, Instant finishedAt);
  void appendMetric(String id, ExperimentMetricRow row);
  void appendDecision(String id, Map<String, ?> decision);
  void appendEvent(String id, String source, String severity, String message);
  List<ExperimentMetricRow> metrics(String id);
  List<Map<String, Object>> events(String id);
  List<Map<String, Object>> decisions(String id);
  ExperimentSummary summary(String id);
  ExperimentSummary finish(String id, CleanupResult cleanup);
}
