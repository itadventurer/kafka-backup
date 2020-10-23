package de.azapps.kafkabackup.common;

import com.amazonaws.services.s3.model.ObjectMetadata;
import de.azapps.kafkabackup.storage.s3.AwsS3Service;
import java.io.ByteArrayInputStream;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public class KafkaConfigWriter {

  private final String BUCKET_NAME_FOR_CONFIG_BACKUP;
  private final AwsS3Service awsS3Service;

  public void storeConfigBackup(TopicsConfig topicsConfig) {
    String fileName = buildFileName(topicsConfig);

    if (awsS3Service.checkIfObjectExists(BUCKET_NAME_FOR_CONFIG_BACKUP, fileName)) {
      log.info("Config file is already stored on S3");
      return;
    }
    byte[] configFile = topicsConfig.toJson().getBytes();
    ByteArrayInputStream inputStream = new ByteArrayInputStream(configFile);
    ObjectMetadata objectMetadata = new ObjectMetadata();
    objectMetadata.setContentLength(configFile.length);

    awsS3Service.saveFile(BUCKET_NAME_FOR_CONFIG_BACKUP, buildFileName(topicsConfig), inputStream, objectMetadata);
  }

  private String buildFileName(TopicsConfig topicsConfig) {
    return topicsConfig.checksum() + ".json";
  }

}
