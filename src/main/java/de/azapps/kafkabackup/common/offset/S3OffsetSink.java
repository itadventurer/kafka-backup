package de.azapps.kafkabackup.common.offset;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import de.azapps.kafkabackup.storage.s3.AwsS3Service;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.RetriableException;

public class S3OffsetSink extends OffsetSink {
    private final String bucketName;
    private final AdminClient adminClient;
    private final AwsS3Service awsS3Service;

    private Map<TopicPartition, OffsetStoreS3File> topicOffsets = new HashMap<>();

    public S3OffsetSink(
        AdminClient adminClient,
        String bucketName,
        AwsS3Service awsS3Service,
        Long consumerGroupsSyncInterval
    ) {
        super(adminClient, consumerGroupsSyncInterval);
        this.bucketName = bucketName;
        this.adminClient = adminClient;
        this.awsS3Service = awsS3Service;
    }

    public void syncOffsetsForGroup(String consumerGroup, Set<TopicPartition> topicPartitions) throws IOException {
        Map<TopicPartition, OffsetAndMetadata> topicOffsetsAndMetadata;
        ListConsumerGroupOffsetsOptions listConsumerGroupOffsetsOptions = new ListConsumerGroupOffsetsOptions();
        listConsumerGroupOffsetsOptions.topicPartitions(Lists.newArrayList(topicPartitions));

        try {
            topicOffsetsAndMetadata = adminClient
                .listConsumerGroupOffsets(consumerGroup, listConsumerGroupOffsetsOptions)
                .partitionsToOffsetAndMetadata()
                .get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RetriableException(e);
        }

        for (TopicPartition tp : topicOffsetsAndMetadata.keySet()) {
            if (!this.topicOffsets.containsKey(tp)) {
                this.topicOffsets.put(tp, new OffsetStoreS3File(tp, bucketName, awsS3Service));
            }

            OffsetStoreS3File offsets = this.topicOffsets.get(tp);
            offsets.put(consumerGroup, topicOffsetsAndMetadata.get(tp).offset());
        }
    }

    public void flush() {
    for (OffsetStoreS3File offsetStoreFile : topicOffsets.values()) {
        try {
            offsetStoreFile.flush();
        } catch (IOException e) {
            throw new RuntimeException("Unable to flush offset store file");
        }
    }
    }

    public void close() {

    }

    private static class OffsetStoreS3File {
        private Map<String, Long> groupOffsets = new HashMap<>();
        private final TopicPartition topicPartition;
        private final String bucketName;


        private ObjectMapper mapper = new ObjectMapper();
        private AwsS3Service awsS3Service;

        OffsetStoreS3File(TopicPartition topicPartition, String bucketName, AwsS3Service awsS3Service) throws IOException {
            this.bucketName = bucketName;
            this.topicPartition = topicPartition;
            this.awsS3Service = awsS3Service;

            if (awsS3Service.checkIfObjectExists(bucketName, getOffsetFileName())) {
                groupOffsets = mapper.readValue(awsS3Service.getFile(bucketName, getOffsetFileName()).getObjectContent(), Map.class);
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