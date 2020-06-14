package de.azapps.kafkabackup.common.offset;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

public class OffsetSource {
    private static final Logger log = LoggerFactory.getLogger(OffsetSource.class);
    private final Map<TopicPartition, OffsetStoreFile> topicOffsets = new HashMap<>();
    private final Map<String, Object> consumerConfig;

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
                topicOffsets.put(topicPartition, new OffsetStoreFile(f));
            }
        }
    }

    public void syncGroupForOffset(TopicPartition topicPartition, long sourceOffset, long targetOffset) {
        OffsetStoreFile offsetStoreFile = topicOffsets.get(topicPartition);
        // __consumer_offsets contains the offset of the message to read next. So we need to search for the offset + 1
        // if we do not do that we might miss
        List<String> groups = offsetStoreFile.groupForOffset(sourceOffset + 1);
        if (groups != null && groups.size() > 0) {
            for (String group : groups) {
                Map<String, Object> groupConsumerConfig = new HashMap<>(consumerConfig);
                groupConsumerConfig.put("group.id", group);
                Consumer<byte[], byte[]> consumer = new KafkaConsumer<>(groupConsumerConfig);
                consumer.assign(Collections.singletonList(topicPartition));
                // ! Target Offset + 1 as we commit the offset of the "next message to read"
                OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(targetOffset + 1);
                Map<TopicPartition, OffsetAndMetadata> offsets = Collections.singletonMap(topicPartition, offsetAndMetadata);
                consumer.commitSync(offsets);
                consumer.close();
                log.debug("Committed target offset {} for group {} for topic {} partition {}",
                        (targetOffset + 1), group, topicPartition.topic(), topicPartition.partition());
            }
        }
    }

    private static class OffsetStoreFile {
        TypeReference<HashMap<String, Long>> typeRef
                = new TypeReference<HashMap<String, Long>>() {
        };
        private final Map<Long, List<String>> offsetGroups = new HashMap<>();

        OffsetStoreFile(Path storeFile) throws IOException {
            ObjectMapper mapper = new ObjectMapper();
            Map<String, Long> groupOffsets = mapper.readValue(storeFile.toFile(), typeRef);
            for (Map.Entry<String, Long> entry : groupOffsets.entrySet()) {
                String group = entry.getKey();
                Long offset = entry.getValue();

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

}

