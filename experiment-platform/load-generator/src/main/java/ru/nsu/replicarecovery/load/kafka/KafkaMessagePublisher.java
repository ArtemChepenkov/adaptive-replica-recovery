package ru.nsu.replicarecovery.load.kafka;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import ru.nsu.replicarecovery.load.core.MessagePublisher;

public final class KafkaMessagePublisher implements MessagePublisher {
  private final String topic;
  private final Producer<byte[], byte[]> producer;

  public KafkaMessagePublisher(String bootstrapServers, String topic) {
    this(topic, createProducer(bootstrapServers, topic));
  }

  KafkaMessagePublisher(String topic, Producer<byte[], byte[]> producer) {
    if (topic == null || topic.isBlank()) {
      throw new IllegalArgumentException("topic must not be blank");
    }
    this.topic = topic;
    this.producer = Objects.requireNonNull(producer);
  }

  static Map<String, Object> producerSettings(String bootstrapServers) {
    return Map.of(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class,
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class,
        ProducerConfig.ACKS_CONFIG, "all",
        ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true,
        ProducerConfig.COMPRESSION_TYPE_CONFIG, "none",
        ProducerConfig.LINGER_MS_CONFIG, 0,
        ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 30_000,
        // Avoid blocking the scheduling thread when metadata or buffer space is unavailable.
        // Such attempts are recorded as errors; later attempts can succeed after metadata arrives.
        ProducerConfig.MAX_BLOCK_MS_CONFIG, 0);
  }

  private static Producer<byte[], byte[]> createProducer(String bootstrapServers, String topic) {
    if (topic == null || topic.isBlank()) {
      throw new IllegalArgumentException("topic must not be blank");
    }
    return new KafkaProducer<>(producerSettings(bootstrapServers));
  }

  @Override
  public void send(long seed, long sequence, byte[] payload, Consumer<Exception> callback) {
    byte[] key = (seed + ":" + sequence).getBytes(StandardCharsets.UTF_8);
    producer.send(new ProducerRecord<>(topic, key, Arrays.copyOf(payload, payload.length)),
        (metadata, error) -> callback.accept(error));
  }

  @Override
  public void close(Duration timeout) {
    producer.close(timeout);
  }
}
