package de.azapps.kafkabackup.common.topic;

import static de.azapps.kafkabackup.common.topic.TestTopicConfigHelper.PROPERTY_1;
import static de.azapps.kafkabackup.common.topic.TestTopicConfigHelper.PROPERTY_2;
import static de.azapps.kafkabackup.common.topic.TestTopicConfigHelper.TOPIC_NAME_1;
import static de.azapps.kafkabackup.common.topic.TestTopicConfigHelper.TOPIC_NAME_2;
import static de.azapps.kafkabackup.common.topic.TestTopicConfigHelper.VALUE_1;
import static de.azapps.kafkabackup.common.topic.TestTopicConfigHelper.VALUE_2;
import static de.azapps.kafkabackup.common.topic.TestTopicConfigHelper.configuration;
import static de.azapps.kafkabackup.common.topic.TestTopicConfigHelper.forTopic;
import static de.azapps.kafkabackup.common.topic.TestTopicConfigHelper.topicConfiguration;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.when;
import de.azapps.kafkabackup.common.AdminClientService;
import de.azapps.kafkabackup.common.KafkaConfigReader;
import de.azapps.kafkabackup.common.TopicConfiguration;
import de.azapps.kafkabackup.common.TopicsConfig;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.common.config.ConfigResource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class KafkaConfigReaderTest {

  private KafkaConfigReader sut;

  @Mock
  AdminClientService adminClientService;

  private static final Set<String> DEFAULT_CONFIG_PROPERTIES = Set.of(PROPERTY_1, PROPERTY_2);

  @BeforeEach
  public void init() {
    sut = new KafkaConfigReader(DEFAULT_CONFIG_PROPERTIES, adminClientService);
  }


  @Test
  public void shouldReturnTopicsConfig() {
    // given
    TopicConfiguration topicConfigurationForFirstTopic = topicConfiguration(TOPIC_NAME_1);
    TopicConfiguration topicConfigurationForSecondTopic = topicConfiguration(TOPIC_NAME_2);

    when(adminClientService.describeAllTopics())
        .thenReturn(List.of(topicConfigurationForFirstTopic, topicConfigurationForSecondTopic));

    Map<ConfigResource, Config> configuration = configuration(
        forTopic(TOPIC_NAME_1).withConfig(PROPERTY_1, VALUE_1),
        forTopic(TOPIC_NAME_2).withConfig(PROPERTY_2, VALUE_2)
    );

    when(adminClientService.getConfigs(List.of(TOPIC_NAME_1, TOPIC_NAME_2))).thenReturn(configuration);

    // when
    TopicsConfig topicsConfigResult = sut.readCurrentConfig();

    // then
    assertEquals(topicsConfigResult.getTopics().size(), configuration.size());

    TopicConfiguration firstTopicConfiguration = topicsConfigResult.getTopics().get(0);
    assertEquals(firstTopicConfiguration.getTopicName(), TOPIC_NAME_1);
    assertEquals(firstTopicConfiguration.getPartitionsNumber(), topicConfigurationForFirstTopic.getPartitionsNumber());
    assertEquals(firstTopicConfiguration.getConfiguration().get(PROPERTY_1), VALUE_1);

    TopicConfiguration secondTopicConfiguration = topicsConfigResult.getTopics().get(1);
    assertEquals(secondTopicConfiguration.getTopicName(), TOPIC_NAME_2);
    assertEquals(secondTopicConfiguration.getPartitionsNumber(), topicConfigurationForSecondTopic.getPartitionsNumber());
    assertEquals(secondTopicConfiguration.getConfiguration().get(PROPERTY_2), VALUE_2);
  }

  @Test
  public void shouldFilterUnwantedConfigProperties2() {
    // given
    sut = new KafkaConfigReader(wantedProperties(PROPERTY_1), adminClientService);

    TopicConfiguration topicConfigurationForFirstTopic = topicConfiguration(TOPIC_NAME_1);
    TopicConfiguration topicConfigurationForSecondTopic = topicConfiguration(TOPIC_NAME_2);

    when(adminClientService.describeAllTopics())
        .thenReturn(List.of(topicConfigurationForFirstTopic, topicConfigurationForSecondTopic));

    Map<ConfigResource, Config> configuration = configuration(
        forTopic(TOPIC_NAME_1).withConfig(PROPERTY_1, VALUE_1),
        forTopic(TOPIC_NAME_2).withConfig(PROPERTY_2, VALUE_2)
    );

    when(adminClientService.getConfigs(List.of(TOPIC_NAME_1, TOPIC_NAME_2))).thenReturn(configuration);

    // when
    TopicsConfig topicsConfigResult = sut.readCurrentConfig();

    // then
    assertEquals(topicsConfigResult.getTopics().size(), configuration.size());

    TopicConfiguration firstTopicConfiguration = topicsConfigResult.getTopics().get(0);
    assertEquals(firstTopicConfiguration.getTopicName(), TOPIC_NAME_1);
    assertEquals(firstTopicConfiguration.getPartitionsNumber(), topicConfigurationForFirstTopic.getPartitionsNumber());
    assertEquals(firstTopicConfiguration.getConfiguration().get(PROPERTY_1), VALUE_1);

    TopicConfiguration secondTopicConfiguration = topicsConfigResult.getTopics().get(1);
    assertEquals(secondTopicConfiguration.getTopicName(), TOPIC_NAME_2);
    assertEquals(secondTopicConfiguration.getPartitionsNumber(), topicConfigurationForSecondTopic.getPartitionsNumber());
    assertFalse(secondTopicConfiguration.getConfiguration().containsKey(PROPERTY_2));
    assertEquals(secondTopicConfiguration.getConfiguration().size(), 0);
  }

  @Test
  public void shouldReturnAllConfigPropertiesAsDefaultBehaviour() {
    // given
    sut = new KafkaConfigReader(null, adminClientService);

    TopicConfiguration topicConfigurationForFirstTopic = topicConfiguration(TOPIC_NAME_1);
    TopicConfiguration topicConfigurationForSecondTopic = topicConfiguration(TOPIC_NAME_2);

    when(adminClientService.describeAllTopics())
        .thenReturn(List.of(topicConfigurationForFirstTopic, topicConfigurationForSecondTopic));

    Map<ConfigResource, Config> configuration = configuration(
        forTopic(TOPIC_NAME_1).withConfig(PROPERTY_1, VALUE_1),
        forTopic(TOPIC_NAME_2).withConfig(PROPERTY_2, VALUE_2)
    );

    when(adminClientService.getConfigs(List.of(TOPIC_NAME_1, TOPIC_NAME_2))).thenReturn(configuration);

    // when
    TopicsConfig topicsConfigResult = sut.readCurrentConfig();

    // then
    assertEquals(topicsConfigResult.getTopics().size(), configuration.size());

    TopicConfiguration firstTopicConfiguration = topicsConfigResult.getTopics().get(0);
    assertEquals(firstTopicConfiguration.getTopicName(), TOPIC_NAME_1);
    assertEquals(firstTopicConfiguration.getPartitionsNumber(), topicConfigurationForFirstTopic.getPartitionsNumber());
    assertEquals(firstTopicConfiguration.getConfiguration().get(PROPERTY_1), VALUE_1);

    TopicConfiguration secondTopicConfiguration = topicsConfigResult.getTopics().get(1);
    assertEquals(secondTopicConfiguration.getTopicName(), TOPIC_NAME_2);
    assertEquals(secondTopicConfiguration.getPartitionsNumber(), topicConfigurationForSecondTopic.getPartitionsNumber());
    assertEquals(secondTopicConfiguration.getConfiguration().get(PROPERTY_2), VALUE_2);
  }

  private Set<String> wantedProperties(String... properties) {
    return Arrays.stream(properties).collect(Collectors.toSet());
  }


}