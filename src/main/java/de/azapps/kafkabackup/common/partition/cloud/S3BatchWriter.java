package de.azapps.kafkabackup.common.partition.cloud;

import com.amazonaws.services.s3.model.ObjectMetadata;
import de.azapps.kafkabackup.common.record.Record;
import de.azapps.kafkabackup.common.record.RecordJSONSerde;
import de.azapps.kafkabackup.storage.s3.AwsS3Service;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.RetriableException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class S3BatchWriter {

    private static final byte[] RECORD_SEPARATOR = {0x0A};  // utf-8 encoding of unix line feed

    private final AwsS3Service awsS3Service;
    private final String bucketName;

    private final String topic;
    private final int partition;

    private final RecordJSONSerde recordJSONSerde = new RecordJSONSerde();
    private final ByteArrayOutputStream buffer = new ByteArrayOutputStream();

    private long startWallClockTime;
    private long startOffset;
    private long endOffset;
    private long count = 0;

    public S3BatchWriter(AwsS3Service awsS3Service, String bucketName, TopicPartition tp, Record record) throws IOException {
        this.awsS3Service = awsS3Service;
        this.bucketName = bucketName;
        this.topic = tp.topic();
        this.partition = tp.partition();
        // Initialize S3BatchWriter with a first record
        this.startWallClockTime = System.currentTimeMillis();
        this.startOffset = record.kafkaOffset();
        append(record);
    }

    public void append(Record record) throws IOException {
        recordJSONSerde.write(buffer, record);
        buffer.write(RECORD_SEPARATOR);
        endOffset = record.kafkaOffset();
        count++;
    }

    public void commitBatch() throws RetriableException {
        // Assume that we can store all content in memory at once
        // TODO: upgrade to >= Java 9 and use InputStream.transferTo() ?
        byte[] batchContent = buffer.toByteArray();
        ByteArrayInputStream inputStream = new ByteArrayInputStream(batchContent);
        ObjectMetadata objectMetadata = new ObjectMetadata();
        objectMetadata.setContentLength(batchContent.length);
        try {
            awsS3Service.saveFile(bucketName, getObjectKey(), inputStream, objectMetadata);
        } catch (RuntimeException e) {
            throw new RetriableException(e);
        }
    }

    public String getObjectKey() {
        return String.format("%s/%03d/msg_%020d.json", topic, partition, startOffset);
    }

    public long getStartOffset() {
        return startOffset;
    }

    public long getEndOffset() {
        return endOffset;
    }

    public long getStartWallClockTime() {
        return startWallClockTime;
    }

    public long getCount() {
        return count;
    }
}
