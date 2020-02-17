package de.azapps.kafkabackup.common.offset;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.AdminClient;

@RequiredArgsConstructor
public class S3OffsetSink implements OffsetSink {
    private final String bucketName;
    private final AdminClient adminClient;


    public void syncConsumerGroups() {

    }

    public void syncOffsets() {

    }

    public void flush() {

    }

    public void close() {

    }
}
