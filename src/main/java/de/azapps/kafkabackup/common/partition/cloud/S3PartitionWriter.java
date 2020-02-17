package de.azapps.kafkabackup.common.partition.cloud;

import com.amazonaws.services.s3.model.ObjectMetadata;
import de.azapps.kafkabackup.common.partition.PartitionException;
import de.azapps.kafkabackup.common.partition.PartitionWriter;
import de.azapps.kafkabackup.common.record.Record;
import de.azapps.kafkabackup.common.record.RecordJSONSerde;
import de.azapps.kafkabackup.storage.s3.AwsS3Service;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import lombok.Builder;
import lombok.RequiredArgsConstructor;
import org.apache.commons.codec.binary.Base64;

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

    String fileName = buildFileNameForRecord(record);
    try {
      byte[] jsonRecord = recordJSONSerde.writeValueAsString(record).getBytes();

      ObjectMetadata objectMetadata = new ObjectMetadata();
      objectMetadata.setContentLength(jsonRecord.length);

      awsS3Service.saveFile(bucketName, fileName, new ByteArrayInputStream(jsonRecord), objectMetadata);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private String buildFileNameForRecord(Record record) {
    return Arrays.toString(Base64.decodeBase64(record.key())) + "_" + record.kafkaOffset();
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
