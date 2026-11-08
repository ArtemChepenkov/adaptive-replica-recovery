package ru.nsu.replicarecovery.runner.client;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.junit.jupiter.api.Test;

class KafkaRecoveryTelemetryTest {
  @Test
  void countsOnlyLiveReplicasAndRequiresIndependentRacks() {
    Node a = new Node(1, "broker-1", 9092, "a");
    Node sameRack = new Node(4, "broker-4", 9092, "a");
    Node b = new Node(2, "broker-2", 9092, "b");
    Node c = new Node(3, "broker-3", 9092, "c");
    var live = List.of(a, sameRack, b);
    var replicas = List.of(a, sameRack, b, c);
    var before = new TopicDescription("recovering", false,
        List.of(new TopicPartitionInfo(0, a, replicas, List.of(a, sameRack, c))));

    assertThat(KafkaRecoveryTelemetry.summarize(live, List.of(before), HttpClientsTest.spec().recovery()))
        .isEqualTo(new KafkaRecoveryTelemetry.RiskObservation(1, false));

    var after = new TopicDescription("recovering", false,
        List.of(new TopicPartitionInfo(0, a, replicas, List.of(a, sameRack, b, c))));
    assertThat(KafkaRecoveryTelemetry.summarize(replicas, List.of(after), HttpClientsTest.spec().recovery()))
        .isEqualTo(new KafkaRecoveryTelemetry.RiskObservation(0, true));
  }

  @Test
  void absentRackDoesNotFabricateHealthyTelemetry() {
    Node withoutRack = new Node(1, "broker-1", 9092);
    var replicas = List.of(withoutRack);
    var topic = new TopicDescription("recovering", false,
        List.of(new TopicPartitionInfo(0, withoutRack, replicas, replicas)));

    assertThat(KafkaRecoveryTelemetry.summarize(replicas, List.of(topic), HttpClientsTest.spec().recovery())).isNull();
  }
}
