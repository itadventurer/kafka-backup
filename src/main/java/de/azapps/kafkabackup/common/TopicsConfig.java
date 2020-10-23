package de.azapps.kafkabackup.common;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import lombok.AccessLevel;
import lombok.Data;
import lombok.EqualsAndHashCode.Exclude;
import lombok.RequiredArgsConstructor;
import org.apache.commons.codec.digest.DigestUtils;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
@Data
public class TopicsConfig {

  @Exclude
  private final long timestamp;
  private final List<TopicConfiguration> topics;
  @JsonIgnore
  private final ObjectMapper mapper = new ObjectMapper();

  public static TopicsConfig of(List<TopicConfiguration> topics) {
    List<TopicConfiguration> newList = new ArrayList<>(topics);
    newList.sort(Comparator.comparing(TopicConfiguration::getTopicName));
    return new TopicsConfig(System.currentTimeMillis(),newList);
  }

  public String checksum() {
    try {
      return DigestUtils.md5Hex(mapper.writeValueAsString(topics));
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Exception was caught when trying to calculate MD5", e);
    }
  }

  public String toJson() {
    try {
      return mapper.writeValueAsString(this);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Exception was caught when trying to parse to JSON", e);
    }
  }

}
