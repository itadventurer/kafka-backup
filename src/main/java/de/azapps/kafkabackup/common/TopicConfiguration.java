package de.azapps.kafkabackup.common;

import java.util.Map;
import java.util.TreeMap;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Getter
public class TopicConfiguration {

  private final String topicName;
  private final int partitionsNumber;
  private Map<String, String> configuration;

  public void setConfiguration(Map<String, String> configuration) {
    this.configuration = new TreeMap<>(configuration);
  }
}
