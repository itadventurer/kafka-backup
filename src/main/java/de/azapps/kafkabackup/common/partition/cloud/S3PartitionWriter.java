package de.azapps.kafkabackup.common.partition.cloud;

import de.azapps.kafkabackup.common.partition.PartitionException;
import de.azapps.kafkabackup.common.partition.PartitionWriter;
import de.azapps.kafkabackup.common.record.Record;
import de.azapps.kafkabackup.storage.s3.AwsS3Service;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.RetriableException;

@Slf4j
@RequiredArgsConstructor
public class S3PartitionWriter implements PartitionWriter {

  private final AwsS3Service awsS3Service;
  private final String bucketName;
  private final TopicPartition topicPartition;
  private final int maxBatchMessages;
  private final long maxBatchTimeMs;
  private final Queue<Record> buffer = new LinkedList<>();

  private S3BatchWriter batchWriter;
  private Long lastCommittableOffset = null; // Nothing written so far, so nothing to commit

  private boolean forceCommit = false;

  @Override
  public void append(Record record) throws PartitionException {
    if (record.kafkaOffset() > batchWriter.getEndOffset()) {
      buffer.add(record);
    } else {
      log.debug("Skipping message already added to buffer.");
      forceCommit = true;
    }
  }

  @Override
  public void flush() throws PartitionException, RetriableException {
    while (!buffer.isEmpty()) {
      Record record = buffer.poll();
      writeToBatch(record);
      maybeCommitBasedOnMessages();
    }

    maybeForceCommitBecauseOfRepeatedOffset(forceCommit);
    maybeCommitBasedOnTime();
  }

  private void writeToBatch(Record record) {
    try {
      if (record.kafkaOffset() <= batchWriter.getEndOffset()) {
        log.debug("Skipping message already added to batch.");
        forceCommit = true;
      }
      if (batchWriter == null) {
        batchWriter = new S3BatchWriter(awsS3Service, bucketName, topicPartition, record);
      } else {
        batchWriter.append(record);
      }
    } catch (IOException e) {
      throw new PartitionException(e);
    }
  }

  private void maybeForceCommitBecauseOfRepeatedOffset(boolean shouldCommit) {
    if (shouldCommit) {
      log.info("Commit {} based on repeated message offset", batchWriter.getObjectKey());
      commitCurrentBatch();
      forceCommit = false;
    }
  }

  private void maybeCommitBasedOnMessages() {
    if (batchWriter != null && batchWriter.getCount() >= maxBatchMessages) {
      log.info("Commit {} based on num messages", batchWriter.getObjectKey());
      commitCurrentBatch();
    }
  }

  private void maybeCommitBasedOnTime() {
    long now = System.currentTimeMillis();
    if (batchWriter != null && (now - batchWriter.getStartWallClockTime()) >= maxBatchTimeMs) {
      log.info("Commit {} based on time", batchWriter.getObjectKey());
      commitCurrentBatch();
    }
  }

  private void commitCurrentBatch() {
    batchWriter.commitBatch();
    // We got here without exception, so its safe to commit to kafka
    lastCommittableOffset = batchWriter.getEndOffset() + 1;
    batchWriter = null;
  }

  @Override
  public void close() throws PartitionException {
  }

  @Override
  public String topic() {
    return topicPartition.topic();
  }

  @Override
  public int partition() {
    return topicPartition.partition();
  }

  @Override
  public Long getLastCommittableOffset() {
    return lastCommittableOffset;
  }
}
