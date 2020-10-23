package de.azapps.kafkabackup.common;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.ConfigEntry;

@RequiredArgsConstructor
@Slf4j
public class KafkaConfigReader {

  private final Set<String> configProperties;
  private final AdminClientService adminClientService;

  public TopicsConfig readCurrentConfig() {
    List<TopicConfiguration> topics = adminClientService.describeAllTopics();

    Map<String, Collection<ConfigEntry>> configDescribeResult = adminClientService.getConfigs(getTopicNames(topics))
        .entrySet()
        .stream()
        .collect(Collectors.toMap(entry -> entry.getKey().name(), entry -> filterProperties(entry.getValue().entries())));

    topics.forEach(topicConfiguration -> topicConfiguration.setConfiguration(
        new TreeMap<>(mapConfigEntries(configDescribeResult.get(topicConfiguration.getTopicName())))));

    return TopicsConfig.of(topics);
  }

  private List<String> getTopicNames(List<TopicConfiguration> topics) {
    return topics.stream().map(TopicConfiguration::getTopicName).collect(Collectors.toList());
  }

  private Collection<ConfigEntry> filterProperties(Collection<ConfigEntry> configEntries) {
    if (configProperties == null) {
      return configEntries;
    }

    return configEntries.stream()
        .peek(entry -> log.debug("Config entry name: {} value: {}", entry.name(), entry.value()))
        .filter(configEntry -> configProperties.contains(configEntry.name()))
        .collect(Collectors.toList());
  }

  private Map<String, String> mapConfigEntries(Collection<ConfigEntry> configEntries) {
    return configEntries.stream()
        .collect(Collectors.toMap(ConfigEntry::name, ConfigEntry::value));
  }

}
