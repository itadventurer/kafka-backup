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
import org.apache.kafka.connect.errors.RetriableException;

@RequiredArgsConstructor
public abstract class OffsetSink {

  private final AdminClient adminClient;
  private List<String> consumerGroups = new ArrayList<>();

  public void syncConsumerGroups() {
    try {
      consumerGroups = adminClient.listConsumerGroups()
          .all().get().stream()
          .map(ConsumerGroupListing::groupId)
          .collect(Collectors.toList());
    } catch (InterruptedException | ExecutionException e) {
      throw new RetriableException(e);
    }
  }

  public void syncOffsets() {
    List<String> listOfExecutionExceptions = consumerGroups.stream()
        .flatMap(consumerGroup -> {
          try {
            syncOffsetsForGroup(consumerGroup);
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

  public abstract void syncOffsetsForGroup(String consumerGroup) throws IOException;
  public abstract void flush();
  public abstract void close();

}
