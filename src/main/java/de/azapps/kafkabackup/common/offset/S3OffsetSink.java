package de.azapps.kafkabackup.common.offset;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.azapps.kafkabackup.storage.s3.AwsS3Service;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

public class S3OffsetSink extends OffsetSink {
    private final AwsS3Service awsS3Service;
    private final String bucketName;

    private Map<TopicPartition, OffsetStoreS3File> topicOffsets = new HashMap<>();

    public S3OffsetSink(
        AdminClient adminClient,
        Long consumerGroupsSyncInterval,
        AwsS3Service awsS3Service,
        String bucketName
    ) {
        super(adminClient, consumerGroupsSyncInterval);
        this.awsS3Service = awsS3Service;
        this.bucketName = bucketName;
    }

    @Override
    public void writeOffsetsForGroup(String consumerGroup, Map<TopicPartition, OffsetAndMetadata> partitionOffsetsAndMetadata) throws IOException {
        for (TopicPartition tp : partitionOffsetsAndMetadata.keySet()) {
            if (!this.topicOffsets.containsKey(tp)) {
                this.topicOffsets.put(tp, new OffsetStoreS3File(tp, bucketName, awsS3Service));
            }

            OffsetStoreS3File offsets = this.topicOffsets.get(tp);
            offsets.put(consumerGroup, partitionOffsetsAndMetadata.get(tp).offset());
        }
    }

    @Override
    public void flush() {
        for (OffsetStoreS3File offsetStoreFile : topicOffsets.values()) {
            try {
                offsetStoreFile.flush();
            } catch (IOException e) {
                throw new RuntimeException("Unable to flush offset store file");
            }
        }
    }

    @Override
    public void close() {
        flush();
    }

    private static class OffsetStoreS3File {
        private static final TypeReference<HashMap<String, Long>> groupOffsetsTypeRef = new TypeReference<HashMap<String, Long>>() {};

        private Map<String, Long> groupOffsets = new HashMap<>();
        private final TopicPartition topicPartition;
        private final String bucketName;
        private final ObjectMapper mapper = new ObjectMapper();
        private final AwsS3Service awsS3Service;

        OffsetStoreS3File(TopicPartition topicPartition, String bucketName, AwsS3Service awsS3Service) throws IOException {
            this.bucketName = bucketName;
            this.topicPartition = topicPartition;
            this.awsS3Service = awsS3Service;

            if (awsS3Service.checkIfObjectExists(bucketName, getOffsetFileName())) {
                groupOffsets = mapper.readValue(awsS3Service.getFile(bucketName, getOffsetFileName()).getObjectContent(), groupOffsetsTypeRef);
            }
        }

        void put(String consumerGroup, long offset) {
            groupOffsets.put(consumerGroup, offset);
        }

        void flush() throws IOException {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            mapper.writeValue(outputStream, groupOffsets);
            byte[] bytes = outputStream.toByteArray();
            ObjectMetadata objectMetadata = new ObjectMetadata();
            objectMetadata.setContentLength(bytes.length);

            awsS3Service.saveFile(bucketName, getOffsetFileName(), new ByteArrayInputStream(bytes), objectMetadata);
        }

        private String getOffsetFileName() {
            return String.format("%s/%03d/offsets.json", topicPartition.topic(), topicPartition.partition());
        }
    }
}
