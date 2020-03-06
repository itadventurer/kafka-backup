package de.azapps.kafkabackup.common.offset;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.RetriableException;

@RequiredArgsConstructor
public abstract class OffsetSink {

  private static final long DEFAULT_SYNC_INTERVAL = 5000;

  private final AdminClient adminClient;
  private final long syncInterval;

  private List<String> consumerGroups = new ArrayList<>();
  private long timeOfLastSync = 0;

  public OffsetSink(AdminClient adminClient) {
    this.adminClient = adminClient;
    this.syncInterval = DEFAULT_SYNC_INTERVAL;
  }

  public void syncConsumerGroups() {
    long currentTime = System.currentTimeMillis();
    if(currentTime - this.timeOfLastSync >= syncInterval) {
      try {
        consumerGroups = adminClient.listConsumerGroups()
            .all().get().stream()
            .map(ConsumerGroupListing::groupId)
            .collect(Collectors.toList());
        
        timeOfLastSync = currentTime;
      } catch (InterruptedException | ExecutionException e) {
        throw new RetriableException(e);
      }
    }
  }

  public void syncOffsets(List<TopicPartition> topicPartitions) {
    List<String> listOfExecutionExceptions = consumerGroups.stream()
        .flatMap(consumerGroup -> {
          try {
            syncOffsetsForGroup(consumerGroup, topicPartitions);
            return Stream.empty();
          } catch (IOException e) {
            return Stream.of(e);
          }
        })
        .map(Throwable::getMessage)
        .collect(Collectors.toList());

    if(!listOfExecutionExceptions.isEmpty()) {
      throw new RuntimeException("At least one exception was caught when trying to sync consumer groups offsets: "
          + String.join("; ", listOfExecutionExceptions));
    }
  }

  public abstract void syncOffsetsForGroup(String consumerGroup, List<TopicPartition> topicPartition) throws IOException;
  public abstract void flush();
  public abstract void close();

}
