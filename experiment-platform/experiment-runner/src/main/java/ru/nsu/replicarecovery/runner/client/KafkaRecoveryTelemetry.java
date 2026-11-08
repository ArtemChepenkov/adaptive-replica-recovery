package ru.nsu.replicarecovery.runner.client;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import ru.nsu.replicarecovery.model.RecoverySpec;

/** Reads current ISR and broker racks directly from Kafka for each observed risk sample. */
final class KafkaRecoveryTelemetry implements AutoCloseable {
  private static final int TIMEOUT_MS = 3000;
  private final Admin admin;

  KafkaRecoveryTelemetry(String bootstrapServers) {
    admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
        AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, TIMEOUT_MS,
        AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, TIMEOUT_MS));
  }

  RiskObservation current(RecoverySpec spec) {
    try {
      var brokers = admin.describeCluster().nodes();
      Pattern excluded = spec.execution() == null || spec.execution().excludedTopics() == null
          ? null : Pattern.compile(spec.execution().excludedTopics());
      var topics = admin.listTopics().names().get(TIMEOUT_MS, TimeUnit.MILLISECONDS).stream()
          .filter(name -> !name.startsWith("__") && (excluded == null || !excluded.matcher(name).matches()))
          .toList();
      if (topics.isEmpty()) return null;
      var descriptions = admin.describeTopics(topics).allTopicNames().get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
      return summarize(brokers.get(TIMEOUT_MS, TimeUnit.MILLISECONDS), descriptions.values(), spec);
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      return null;
    } catch (ExecutionException | TimeoutException unavailable) {
      return null;
    }
  }

  static RiskObservation summarize(Collection<Node> brokers, Collection<TopicDescription> topics, RecoverySpec spec) {
    if (brokers.isEmpty() || topics.isEmpty()) return null;
    Map<Integer, Node> live = new HashMap<>();
    brokers.forEach(node -> live.put(node.id(), node));
    int critical = 0;
    int partitions = 0;
    boolean safe = true;
    for (TopicDescription topic : topics) {
      for (var partition : topic.partitions()) {
        partitions++;
        int copies = 0;
        var racks = new HashSet<String>();
        for (Node member : partition.isr()) {
          Node broker = live.get(member.id());
          if (broker != null) {
            copies++;
            if (broker.rack() == null || broker.rack().isBlank()) return null;
            racks.add(broker.rack());
          }
        }
        if (copies <= 1 || racks.size() == 1) critical++;
        safe &= copies >= spec.requiredCopies() && racks.size() >= spec.requiredDomains();
      }
    }
    return partitions == 0 ? null : new RiskObservation(critical, safe);
  }

  @Override public void close() { admin.close(Duration.ofSeconds(1)); }

  record RiskObservation(int criticalPartitions, boolean safe) { }
}
