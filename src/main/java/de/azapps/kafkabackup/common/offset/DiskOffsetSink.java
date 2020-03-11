package de.azapps.kafkabackup.common.offset;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

public class DiskOffsetSink extends OffsetSink {
    private final Path targetDir;
    private Map<TopicPartition, OffsetStoreFile> topicOffsets = new HashMap<>();
    private AdminClient adminClient;

    public DiskOffsetSink(AdminClient adminClient, long consumerGroupMaxAgeMs, Path targetDir) {
        super(adminClient, consumerGroupMaxAgeMs);
        this.adminClient = adminClient;
        this.targetDir = targetDir;
    }

    @Override
    public void writeOffsetsForGroup(String consumerGroup, Map<TopicPartition, OffsetAndMetadata> partitionOffsetsAndMetadata) throws IOException {
        for (TopicPartition tp : partitionOffsetsAndMetadata.keySet()) {
            if (validTopic(tp.topic())) {
                if (!this.topicOffsets.containsKey(tp)) {
                    this.topicOffsets.put(tp, new OffsetStoreFile(targetDir, tp));
                }
                OffsetStoreFile offsets = this.topicOffsets.get(tp);
                offsets.put(consumerGroup, partitionOffsetsAndMetadata.get(tp).offset());
            }
        }
    }

    private boolean validTopic(String topic) {
        return Files.isDirectory(Paths.get(targetDir.toString(), topic));
    }

    @Override
    public void flush() {
        boolean error = false;
        for (OffsetStoreFile offsetStoreFile : topicOffsets.values()) {
            try {
                offsetStoreFile.flush();
            } catch (IOException e) {
                e.printStackTrace();
                error = true;
            }
        }
        if (error) {
            throw new RuntimeException("syncOffsets() threw an IOException");
        }
    }

    @Override
    public void close() {
        flush();
    }

    private static class OffsetStoreFile {
        private static final TypeReference<HashMap<String, Long>> groupOffsetsTypeRef = new TypeReference<HashMap<String, Long>>() {};
        private Map<String, Long> groupOffsets = new HashMap<>();

        private ObjectMapper mapper = new ObjectMapper();
        private Path storeFile;

        OffsetStoreFile(Path targetDir, TopicPartition topicPartition) throws IOException {
            storeFile = OffsetUtils.offsetStoreFile(targetDir, topicPartition);
            if (!Files.isRegularFile(storeFile)) {
                Files.createFile(storeFile);
            }
            if (Files.size(storeFile) > 0) {
                groupOffsets = mapper.readValue(storeFile.toFile(), groupOffsetsTypeRef);
            }
        }

        void put(String consumerGroup, long offset) {
            groupOffsets.put(consumerGroup, offset);
        }

        void flush() throws IOException {
            mapper.writeValue(storeFile.toFile(), groupOffsets);
        }
    }
}
