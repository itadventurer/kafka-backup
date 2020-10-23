package de.azapps.kafkabackup.common.topic;

import static de.azapps.kafkabackup.common.topic.TestTopicConfigHelper.PROPERTY_1;
import static de.azapps.kafkabackup.common.topic.TestTopicConfigHelper.PROPERTY_2;
import static de.azapps.kafkabackup.common.topic.TestTopicConfigHelper.TOPIC_NAME_1;
import static de.azapps.kafkabackup.common.topic.TestTopicConfigHelper.TOPIC_NAME_2;
import static de.azapps.kafkabackup.common.topic.TestTopicConfigHelper.VALUE_1;
import static de.azapps.kafkabackup.common.topic.TestTopicConfigHelper.VALUE_2;
import static de.azapps.kafkabackup.common.topic.TestTopicConfigHelper.forTestTopic;
import static org.junit.jupiter.api.Assertions.assertEquals;
import de.azapps.kafkabackup.common.TopicConfiguration;
import de.azapps.kafkabackup.common.TopicsConfig;
import java.util.List;
import org.junit.jupiter.api.Test;

class TopicsConfigTest {

  @Test
  public void shouldCalculateTheSameChecksumForTheSameConfigs() {
    // given
    TopicConfiguration configForFirstTopic = forTestTopic(TOPIC_NAME_1)
        .withNumberOfPartitions(3)
        .withConfig(PROPERTY_1, VALUE_1)
        .withConfig(PROPERTY_2, VALUE_2)
        .build();

    TopicConfiguration configForSecondTopic = forTestTopic(TOPIC_NAME_2)
        .withNumberOfPartitions(3)
        .withConfig(PROPERTY_1, VALUE_1)
        .withConfig(PROPERTY_2, VALUE_2)
        .build();

    // when
    String firstChecksumResult = TopicsConfig.of(List.of(configForFirstTopic, configForSecondTopic)).checksum();

    String secondChecksumResult = TopicsConfig.of(List.of(configForFirstTopic, configForSecondTopic)).checksum();

    // then
    assertEquals(firstChecksumResult, secondChecksumResult);
  }

  @Test
  public void shouldCalculateTheSameChecksumForTheSameConfigsEvenIfOrderOfTopicsIsDifferent() {
    // given
    TopicConfiguration configForFirstTopic = forTestTopic(TOPIC_NAME_1)
        .withConfig(PROPERTY_1, VALUE_1)
        .withConfig(PROPERTY_2, VALUE_2)
        .build();

    TopicConfiguration configForSecondTopic = forTestTopic(TOPIC_NAME_2)
        .withConfig(PROPERTY_1, VALUE_1)
        .withConfig(PROPERTY_2, VALUE_2)
        .build();

    // when
    String firstChecksumResult =
        TopicsConfig.of(List.of(configForFirstTopic, configForSecondTopic)).checksum();

    String secondChecksumResult =
        TopicsConfig.of(List.of(configForSecondTopic, configForFirstTopic)).checksum();

    // then
    assertEquals(firstChecksumResult, secondChecksumResult);
  }

  @Test
  public void shouldCalculateTheSameChecksumForTheSameConfigsEvenIfOrderOfPropertiesIsDifferent() {
    // given
    TopicConfiguration configForFirstTopic = forTestTopic(TOPIC_NAME_1)
        .withNumberOfPartitions(3)
        .withConfig(PROPERTY_1, VALUE_1)
        .withConfig(PROPERTY_2, VALUE_2)
        .build();

    TopicConfiguration configForFirstTopicWithDifferentOrder = forTestTopic(TOPIC_NAME_1)
        .withNumberOfPartitions(3)
        .withConfig(PROPERTY_2, VALUE_2)
        .withConfig(PROPERTY_1, VALUE_1)
        .build();

    // when
    String firstChecksumResult =
        TopicsConfig.of(List.of(configForFirstTopic)).checksum();

    String secondChecksumResult =
        TopicsConfig.of(List.of(configForFirstTopicWithDifferentOrder)).checksum();

    // then
    assertEquals(firstChecksumResult, secondChecksumResult);
  }
}