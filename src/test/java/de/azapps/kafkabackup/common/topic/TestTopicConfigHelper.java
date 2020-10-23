package de.azapps.kafkabackup.common.topic;

import static org.apache.kafka.common.config.ConfigResource.Type.TOPIC;
import de.azapps.kafkabackup.common.TopicConfiguration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.config.ConfigResource;

public class TestTopicConfigHelper {

  public static final String PROPERTY_1 = "property1";
  public static final String VALUE_1 = "value1";

  public static final String PROPERTY_2 = "property2";
  public static final String VALUE_2 = "value";

  public static final String TOPIC_NAME_1 = "topic1";
  public static final String TOPIC_NAME_2 = "topic2";

  public static TopicConfiguration topicConfiguration(String topicName) {
    return new TopicConfiguration(topicName, new Random().ints().findFirst().getAsInt());
  }

  public static Map<ConfigResource, Config> configuration(TestTopicConfiguration... testTopicConfigurations){
    return Arrays.stream(testTopicConfigurations)
        .collect(Collectors.toMap(testTopicConfig -> new ConfigResource(TOPIC, testTopicConfig.topicName),
            testTopicConfig -> new Config(testTopicConfig.getConfigEntries())));
  }

  public static TestTopicConfiguration forTopic(String topicName) {
    return new TestTopicConfiguration(topicName);
  }

  public static TestTopicConfiguration forTestTopic(String topicName) {
    return new TestTopicConfiguration(topicName);
  }

  @Getter
  @RequiredArgsConstructor
  public static class TestTopicConfiguration {
    private final String topicName;
    private int partitionsNumber = new Random().ints().findFirst().getAsInt();
    private final Map<String, String> config = new TreeMap<>();

    public TestTopicConfiguration withNumberOfPartitions(int numberOfPartitions) {
      this.partitionsNumber = numberOfPartitions;
      return this;
    }

    public TestTopicConfiguration withConfig(String key, String value) {
      this.config.put(key, value);
      return this;
    }

    public Collection<ConfigEntry> getConfigEntries() {
      return config.entrySet().stream().map(entry-> new ConfigEntry(entry.getKey(), entry.getValue()))
          .collect(Collectors.toList());
    }

    public TopicConfiguration build() {
      TopicConfiguration topicConfiguration = new TopicConfiguration(topicName, partitionsNumber);
      topicConfiguration.setConfiguration(config);
      return topicConfiguration;
    }

  }
}
