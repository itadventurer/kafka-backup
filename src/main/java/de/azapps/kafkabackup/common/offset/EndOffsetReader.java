package de.azapps.kafkabackup.common.offset;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.util.*;

public class EndOffsetReader {
  private final String bootstrapServers;

  public EndOffsetReader(String bootstrapServers) {
    this.bootstrapServers = bootstrapServers;
  }

  /**
   * Obtain end offsets for each given partition
   */
  public Map<TopicPartition, Long> getEndOffsets(Collection<TopicPartition> partitions) {
    Properties props = new Properties();
    props.put("bootstrap.servers", bootstrapServers);
    props.put("key.deserializer", ByteArrayDeserializer.class.getName());
    props.put("value.deserializer", ByteArrayDeserializer.class.getName());
    KafkaConsumer<Byte[], Byte[]> consumer = new KafkaConsumer<>(props);
    consumer.assign(partitions);
    Map<TopicPartition, Long> offsets = consumer.endOffsets(partitions);
    List<TopicPartition> toRemove = new ArrayList<>();

    for (Map.Entry<TopicPartition, Long> partitionOffset: offsets.entrySet()) {
      if (partitionOffset.getValue() == 0L) {
        toRemove.add(partitionOffset.getKey()); // don't store empty offsets
      }
    }

    for (TopicPartition partition: toRemove) {
      offsets.remove(partition);
    }
    consumer.close();
    return offsets;
  }
}
