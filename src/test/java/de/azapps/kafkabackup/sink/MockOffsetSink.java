package de.azapps.kafkabackup.sink;

import de.azapps.kafkabackup.common.offset.OffsetSink;
import org.apache.kafka.clients.admin.AdminClient;

import java.io.IOException;
import java.nio.file.Path;

public class MockOffsetSink extends OffsetSink {
    public MockOffsetSink(AdminClient adminClient, Path targetDir) {
        super(adminClient, targetDir);
    }

    @Override
    public void syncConsumerGroups() {

    }

    @Override
    public void syncOffsets() throws IOException {
    }

    @Override
    public void flush() throws IOException {
    }

    @Override
    public void close() throws IOException {
    }
}

