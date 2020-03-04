package de.azapps.kafkabackup.common.partition.cloud;

import com.amazonaws.services.s3.model.ObjectMetadata;
import de.azapps.kafkabackup.common.partition.PartitionException;
import de.azapps.kafkabackup.common.partition.PartitionWriter;
import de.azapps.kafkabackup.common.record.Record;
import de.azapps.kafkabackup.common.record.RecordJSONSerde;
import de.azapps.kafkabackup.storage.s3.AwsS3Service;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import lombok.Builder;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Builder
public class S3PartitionWriter implements PartitionWriter {

  private final String bucketName;
  private final int partition;

  private final String topicName;
  private final AwsS3Service awsS3Service;

  private final RecordJSONSerde recordJSONSerde = new RecordJSONSerde();

  private final long lastWrittenOffset = 0;


  @Override
  public void append(Record record) throws PartitionException {
    String fileName = buildFileKeyForRecord(topicName, partition, record);
    try {
      ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
      recordJSONSerde.write(outputStream, record);
      byte[] jsonRecord = outputStream.toByteArray();
      ByteArrayInputStream jsonStream = new ByteArrayInputStream(jsonRecord);

      ObjectMetadata objectMetadata = new ObjectMetadata();
      objectMetadata.setContentLength(jsonRecord.length);

      awsS3Service.saveFile(bucketName, fileName, jsonStream, objectMetadata);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private String buildFileKeyForRecord(String topic, int partition, Record record) {
    return String.format("%s/%03d/msg_%020d.json", topic, partition, record.kafkaOffset());
  }

  @Override
  public void close() throws PartitionException {

  }

  @Override
  public void flush() throws PartitionException {

  }

  @Override
  public long lastWrittenOffset() {
    return lastWrittenOffset;
  }

  @Override
  public String topic() {
    return topicName;
  }

  @Override
  public int partition() {
    return partition;
  }
}
