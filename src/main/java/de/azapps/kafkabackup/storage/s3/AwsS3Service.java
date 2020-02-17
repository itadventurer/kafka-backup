package de.azapps.kafkabackup.storage.s3;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.google.common.base.Strings;
import java.io.InputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AwsS3Service {

  private static final Logger log = LoggerFactory.getLogger(AwsS3Service.class);
  private final AmazonS3 s3Client;

  public AwsS3Service(String region, String endpoint, boolean withPathStyleAccessEnabled) {
    log.debug("Initializing new AWS S3 service (region: {}, endpoint: {}, withPathStyleAccessEnabled: {})",
        region, endpoint, withPathStyleAccessEnabled);
    final AmazonS3ClientBuilder amazonS3ClientBuilder =
        AmazonS3ClientBuilder
            .standard()
            .withPathStyleAccessEnabled(withPathStyleAccessEnabled);

    if (!Strings.isNullOrEmpty(endpoint)) {
      amazonS3ClientBuilder.withEndpointConfiguration(new EndpointConfiguration(endpoint, region));
    } else {
      amazonS3ClientBuilder.withRegion(region);
    }

    s3Client = amazonS3ClientBuilder.build();
  }

  public S3Object getFile(String bucketName, String fileName) {
    log.debug("Getting file from S3 (bucketName: {}, fileName: {})", bucketName, fileName);
    return getObject(new GetObjectRequest(bucketName, fileName));
  }

  public PutObjectResult saveFile(String bucketName, String fileName, InputStream inputStream,
      ObjectMetadata metadata) {
    return putObject(new PutObjectRequest(bucketName, fileName, inputStream, metadata));
  }

  private S3Object getObject(GetObjectRequest getObjectRequest) {
    try {
      return s3Client.getObject(getObjectRequest);
    } catch (AmazonClientException e) {
      throw runtimeException("get file", e);
    }
  }

  private PutObjectResult putObject(PutObjectRequest putObjectRequest) {
    try {
      return s3Client.putObject(putObjectRequest);
    } catch (AmazonClientException e) {
      throw runtimeException("save file", e);
    }
  }

  private RuntimeException runtimeException(String action, AmazonClientException e) {
    if (e instanceof SdkClientException) {
      return new RuntimeException(
          "[AWS S3] Exception was caught when trying to " + action + " -  Amazon S3 couldn't be contacted for a "
              + "response, or the client couldn't parse the response from Amazon S3. Error details: " + e
              .getMessage(), e);
    } else if (e instanceof AmazonServiceException) {
      return new RuntimeException("[AWS S3] Exception was caught when trying to " + action + " - "
          + "The call was transmitted successfully, but Amazon S3 couldn't process\n"
          + " it, so it returned an error response. Error details: " + e.getMessage(), e);
    }
    return new RuntimeException("Exception when " + action, e);
  }
}
