package ru.nsu.replicarecovery.load.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;

class KafkaMessagePublisherTest {
  @Test
  void usesAcknowledgedIdempotentUncompressedSendsWithoutBlockingScheduler() {
    var settings = KafkaMessagePublisher.producerSettings("localhost:19092");

    assertThat(settings).containsEntry(ProducerConfig.ACKS_CONFIG, "all")
        .containsEntry(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true)
        .containsEntry(ProducerConfig.COMPRESSION_TYPE_CONFIG, "none")
        .containsEntry(ProducerConfig.LINGER_MS_CONFIG, 0)
        .containsEntry(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 30_000)
        .containsEntry(ProducerConfig.MAX_BLOCK_MS_CONFIG, 0);
    assertThat(new ProducerConfig(settings).getInt(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG))
        .isLessThanOrEqualTo(30_000);
  }

  @Test
  void copiesPayloadAndBuildsReproducibleKeys() {
    MockProducer<byte[], byte[]> producer = producer();
    KafkaMessagePublisher publisher = new KafkaMessagePublisher("recovery-data", producer);
    byte[] payload = {1, 2, 3};
    List<Exception> completions = new ArrayList<>();

    publisher.send(42, 0, payload, completions::add);
    publisher.send(42, 1, payload, completions::add);
    payload[0] = 9;

    assertThat(producer.history()).hasSize(2);
    assertThat(producer.history().get(0).topic()).isEqualTo("recovery-data");
    assertThat(producer.history().get(0).key())
        .isEqualTo("42:0".getBytes(StandardCharsets.UTF_8));
    assertThat(producer.history().get(1).key())
        .isEqualTo("42:1".getBytes(StandardCharsets.UTF_8));
    assertThat(producer.history().get(0).value()).containsExactly((byte) 1, (byte) 2, (byte) 3);
    assertThat(producer.history().get(1).value()).isNotSameAs(producer.history().get(0).value());
    assertThat(completions).isEmpty();
    producer.completeNext();
    KafkaException failure = new KafkaException("broker unavailable");
    producer.errorNext(failure);
    assertThat(completions).containsExactly(null, failure);
    publisher.close(Duration.ZERO);
    assertThat(producer.closed()).isTrue();
  }

  private static MockProducer<byte[], byte[]> producer() {
    return new MockProducer<>(false, null, new ByteArraySerializer(), new ByteArraySerializer());
  }
}
