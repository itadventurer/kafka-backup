package de.azapps.kafkabackup.common;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.ConfigResource.Type;

@RequiredArgsConstructor
public class AdminClientService {

  private final AdminClient adminClient;

  public List<TopicConfiguration> describeAllTopics() {
    ListTopicsOptions options = new ListTopicsOptions().listInternal(false);
    try {
      Set<String> topicNames = adminClient.listTopics(options).names().get();
      Map<String, TopicDescription> topicsDescription = adminClient.describeTopics(topicNames).all().get();
      return topicsDescription.entrySet().stream()
          .map(entry -> new TopicConfiguration(entry.getKey(), entry.getValue().partitions().size()))
          .collect(Collectors.toList());
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e.getMessage(), e);
    }
  }

  public Map<ConfigResource, Config> getConfigs(List<String> topicNames) {
    try {
    List<ConfigResource> configResources = topicNames.stream()
        .map(topicName -> new ConfigResource(Type.TOPIC, topicName))
        .collect(Collectors.toList());

      return adminClient.describeConfigs(configResources).all().get();
    } catch (InterruptedException | ExecutionException e) {
      e.printStackTrace();
      throw new RuntimeException("Exception was caught when trying to get config for all provided topics", e);
    }
  }

}
