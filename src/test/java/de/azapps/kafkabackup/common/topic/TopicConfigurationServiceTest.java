package de.azapps.kafkabackup.common.topic;

import static de.azapps.kafkabackup.common.topic.TestTopicConfigHelper.PROPERTY_1;
import static de.azapps.kafkabackup.common.topic.TestTopicConfigHelper.PROPERTY_2;
import static de.azapps.kafkabackup.common.topic.TestTopicConfigHelper.TOPIC_NAME_1;
import static de.azapps.kafkabackup.common.topic.TestTopicConfigHelper.TOPIC_NAME_2;
import static de.azapps.kafkabackup.common.topic.TestTopicConfigHelper.VALUE_1;
import static de.azapps.kafkabackup.common.topic.TestTopicConfigHelper.VALUE_2;
import static de.azapps.kafkabackup.common.topic.TestTopicConfigHelper.forTestTopic;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import de.azapps.kafkabackup.common.TopicsConfig;
import de.azapps.kafkabackup.common.KafkaConfigReader;
import de.azapps.kafkabackup.common.KafkaConfigWriter;
import de.azapps.kafkabackup.common.TopicConfiguration;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class TopicConfigurationServiceTest {

  private final long MIN_INTERVAL_MS = 1000L;

  private static final TopicConfiguration configForFirstTopic = forTestTopic(TOPIC_NAME_1)
      .withNumberOfPartitions(3)
      .withConfig(PROPERTY_1, VALUE_1)
      .withConfig(PROPERTY_2, VALUE_2)
      .build();

  private static final TopicConfiguration changedConfigForFirstTopic = forTestTopic(TOPIC_NAME_1)
      .withNumberOfPartitions(10)
      .withConfig(PROPERTY_1, VALUE_1)
      .build();

  private static final TopicConfiguration configForSecondTopic = forTestTopic(TOPIC_NAME_2)
      .withNumberOfPartitions(3)
      .withConfig(PROPERTY_1, VALUE_1)
      .withConfig(PROPERTY_2, VALUE_2)
      .build();

  private static TopicsConfig kafkaTopicConfiguration;
  private static TopicsConfig kafkaTopicNewConfiguration;

  @Mock
  KafkaConfigReader kafkaConfigReader;

  @Mock
  KafkaConfigWriter kafkaConfigWriter;

  @BeforeAll
  public static void initTests() {
    kafkaTopicConfiguration = TopicsConfig.of(List.of(configForFirstTopic, configForSecondTopic));
    kafkaTopicNewConfiguration = TopicsConfig.of(List.of(changedConfigForFirstTopic, configForSecondTopic));
  }

  private TopicConfigurationService initTopicConfigurationService(long minInterval) {
    return new TopicConfigurationService(minInterval, kafkaConfigReader, kafkaConfigWriter);
  }

  @Test
  public void whenRunConfigurationCheck_thenConfigWrittenByWriter() {
    // given
    TopicConfigurationService topicConfigurationService = initTopicConfigurationService(MIN_INTERVAL_MS);
    when(kafkaConfigReader.readCurrentConfig()).thenReturn(kafkaTopicConfiguration);

    // when
    topicConfigurationService.runTopicConfigurationCheck();

    // then
    ArgumentCaptor<TopicsConfig> configuration = ArgumentCaptor.forClass(TopicsConfig.class);
    verify(kafkaConfigWriter, times(1)).storeConfigBackup(configuration.capture());

    assertEquals(kafkaTopicConfiguration.checksum(), configuration.getValue().checksum());
  }

  @Test
  public void whenRunConfigurationCheckCalledTwice_thenConfigWrittenByWriterOnce() throws InterruptedException {
    // given
    TopicConfigurationService topicConfigurationService = initTopicConfigurationService(MIN_INTERVAL_MS);
    when(kafkaConfigReader.readCurrentConfig()).thenReturn(kafkaTopicConfiguration);

    // when
    topicConfigurationService.runTopicConfigurationCheck();
    Thread.sleep(100L);
    topicConfigurationService.runTopicConfigurationCheck();

    // then
    verify(kafkaConfigWriter, times(1)).storeConfigBackup(eq(kafkaTopicConfiguration));
    verify(kafkaConfigReader, times(1)).readCurrentConfig();
  }

  @Test
  public void whenRunConfigurationCheckCalledTwiceWithIntervalElapsedButSameConfig_thenConfigWrittenByWriterOnce()
      throws InterruptedException {
    // given
    TopicConfigurationService topicConfigurationService = initTopicConfigurationService(0L);
    when(kafkaConfigReader.readCurrentConfig()).thenReturn(kafkaTopicConfiguration);

    // when
    topicConfigurationService.runTopicConfigurationCheck();
    Thread.sleep(100L);
    topicConfigurationService.runTopicConfigurationCheck();

    // then
    verify(kafkaConfigWriter, times(1)).storeConfigBackup(eq(kafkaTopicConfiguration));
    verify(kafkaConfigReader, times(2)).readCurrentConfig();
  }

  @Test
  public void whenRunConfigurationCheckCalledTwiceWithIntervalElapsedAndDifferentConfig_thenConfigWrittenByWriterTwice()
      throws InterruptedException {
    // given
    TopicConfigurationService topicConfigurationService = initTopicConfigurationService(0L);
    when(kafkaConfigReader.readCurrentConfig()).thenReturn(kafkaTopicConfiguration)
        .thenReturn(kafkaTopicNewConfiguration);

    // when
    topicConfigurationService.runTopicConfigurationCheck();
    Thread.sleep(200L);
    topicConfigurationService.runTopicConfigurationCheck();
    // then
    verify(kafkaConfigReader, times(2)).readCurrentConfig();
    verify(kafkaConfigWriter, times(1)).storeConfigBackup(eq(kafkaTopicConfiguration));
    verify(kafkaConfigWriter, times(1)).storeConfigBackup(eq(kafkaTopicNewConfiguration));
  }

}
