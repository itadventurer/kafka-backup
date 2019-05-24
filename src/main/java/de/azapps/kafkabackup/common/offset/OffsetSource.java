package de.azapps.kafkabackup.common.offset;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.azapps.kafkabackup.common.partition.PartitionIndex;
import de.azapps.kafkabackup.common.partition.PartitionReader;
import de.azapps.kafkabackup.common.segment.SegmentIndex;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class OffsetSource {
    private Map<TopicPartition, OffsetStoreFile> topicOffsets = new HashMap<>();
    private Map<String, Object> consumerConfig;

    public OffsetSource(Path backupDir, List<String> topics, Map<String, Object> consumerConfig) throws IOException {
        this.consumerConfig = consumerConfig;
        for (String topic : topics) {
            findOffsetStores(backupDir, topic);
        }
    }

    private void findOffsetStores(Path backupDir, String topic) throws IOException {
        Path topicDir = Paths.get(backupDir.toString(), topic);
        for (Path f : Files.list(topicDir).collect(Collectors.toList())) {
            Optional<Integer> partition = OffsetUtils.isOffsetStoreFile(f);
            if (partition.isPresent()) {
                TopicPartition topicPartition = new TopicPartition(topic, partition.get());
                topicOffsets.put(topicPartition, new OffsetSource.OffsetStoreFile(f));
            }
        }
    }

    public void syncGroupForOffset(TopicPartition topicPartition, long offset) throws IOException {
        OffsetStoreFile offsetStoreFile = topicOffsets.get(topicPartition);
        List<String> groups = offsetStoreFile.groupForOffset(offset);
        if (groups != null && groups.size() > 0) {
            for (String group : groups) {
                Map<String, Object> groupConsumerConfig = new HashMap<>(consumerConfig);
                groupConsumerConfig.put("group.id", group);
                Consumer<byte[], byte[]> consumer = new KafkaConsumer<>(groupConsumerConfig);
                consumer.assign(Collections.singletonList(topicPartition));
                OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(offset);
                Map<TopicPartition, OffsetAndMetadata> offsets = Collections.singletonMap(topicPartition, offsetAndMetadata);
                consumer.commitSync(offsets);
                consumer.close();
            }
        }
    }

    private class OffsetStoreFile {
        private Map<Long, List<String>> offsetGroups = new HashMap<>();

        private ObjectMapper mapper = new ObjectMapper();

        OffsetStoreFile(Path storeFile) throws IOException {
            Map<String, Long> groupOffsets = mapper.readValue(storeFile.toFile(), Map.class);
            for (String group : groupOffsets.keySet()) {
                Long offset = groupOffsets.get(group);
                if (offsetGroups.containsKey(offset)) {
                    List<String> groups = offsetGroups.get(offset);
                    groups.add(group);
                } else {
                    List<String> groups = new ArrayList<>(1);
                    groups.add(group);
                    offsetGroups.put(offset, groups);
                }
            }
        }

        List<String> groupForOffset(Long offset) {
            return offsetGroups.get(offset);
        }
    }

    public static class OffsetException extends Exception {
        OffsetException(String message) {
            super(message);
        }
    }
}

