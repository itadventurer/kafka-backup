package de.azapps.kafkabackup.common.offset;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.util.*;

public class EndOffsetReader {
  private final Map<String, Object> consumerConfig;

  public EndOffsetReader(Map<String, Object> consumerConfig) {
    this.consumerConfig = consumerConfig;
  }

  /**
   * Obtain end offsets for each given partition
   */
  public Map<TopicPartition, Long> getEndOffsets(Collection<TopicPartition> partitions) {
    Map<String, Object> serializerConfig = new HashMap<>(consumerConfig);
    serializerConfig.put("key.deserializer", ByteArrayDeserializer.class.getName());
    serializerConfig.put("value.deserializer", ByteArrayDeserializer.class.getName());
    try (KafkaConsumer<Byte[], Byte[]> consumer = new KafkaConsumer<>(serializerConfig)) {
      consumer.assign(partitions);

      Map<TopicPartition, Long> offsets = consumer.endOffsets(partitions);
      List<TopicPartition> toRemove = new ArrayList<>();

      for (Map.Entry<TopicPartition, Long> partitionOffset : offsets.entrySet()) {
        if (partitionOffset.getValue() == 0L) {
          toRemove.add(partitionOffset.getKey()); // don't store empty offsets
        }
      }

      for (TopicPartition partition : toRemove) {
        offsets.remove(partition);
      }

      return offsets;
    }
  }
}
