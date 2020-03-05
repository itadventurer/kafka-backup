package de.azapps.kafkabackup.common.record;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.apache.kafka.common.record.TimestampType;

import static org.junit.jupiter.api.Assertions.*;

public class RecordTest {

    private static final String topic = "test-topic";
    private static final int partition = 42;
    private static final long offset = 123;
    private static final TimestampType timestampType = TimestampType.LOG_APPEND_TIME;
    private static final Long timestamp = 573831430000L;
    // encoding here is not really important, we just want some bytes
    private static final byte[] keyBytes = "test-key".getBytes(StandardCharsets.UTF_8);
    private static final byte[] valueBytes = "test-value".getBytes(StandardCharsets.UTF_8);
    // Header fixtures:
    private static final String header0Key = "header0-key";
    private static final String header1Key = "header1-key";
    private static final String header2Key = "header2-key";
    private static final byte[] header0ValueBytes = "header0-value".getBytes(StandardCharsets.UTF_8);
    private static final byte[] header1ValueBytes = "header1-value".getBytes(StandardCharsets.UTF_8);
    private static final byte[] header2ValueBytes = null;
    private static final ConnectHeaders headers = new ConnectHeaders();
    static {
        headers.add(header0Key, new SchemaAndValue(Schema.OPTIONAL_BYTES_SCHEMA, header0ValueBytes));
        headers.add(header1Key, new SchemaAndValue(Schema.OPTIONAL_BYTES_SCHEMA, header1ValueBytes));
        headers.add(header2Key, new SchemaAndValue(Schema.OPTIONAL_BYTES_SCHEMA, header2ValueBytes));
    }

    @Test
    public void equalsIdentityTrueTest() {
        // GIVEN
        Record a = new Record(topic, partition, keyBytes, valueBytes, offset, timestamp, timestampType, headers);

        // THEN
        assertEquals(a, a);
    }

    @Test
    public void equalsValueTrueTest() {
        // GIVEN
        ConnectHeaders aHeaders = new ConnectHeaders();
        aHeaders.add("header0-key", new SchemaAndValue(Schema.BYTES_SCHEMA, Arrays.copyOf(header0ValueBytes, header0ValueBytes.length)));
        aHeaders.add("header1-key", new SchemaAndValue(Schema.BYTES_SCHEMA, Arrays.copyOf(header1ValueBytes, header1ValueBytes.length)));
        aHeaders.add("header2-key", new SchemaAndValue(Schema.OPTIONAL_BYTES_SCHEMA, null));
        Record a = new Record(topic, partition, keyBytes, valueBytes, offset, timestamp, timestampType, aHeaders);

        ConnectHeaders bHeaders = new ConnectHeaders();
        bHeaders.add("header0-key", new SchemaAndValue(Schema.BYTES_SCHEMA, Arrays.copyOf(header0ValueBytes, header0ValueBytes.length)));
        bHeaders.add("header1-key", new SchemaAndValue(Schema.BYTES_SCHEMA, Arrays.copyOf(header1ValueBytes, header1ValueBytes.length)));
        bHeaders.add("header2-key", new SchemaAndValue(Schema.OPTIONAL_BYTES_SCHEMA, null));
        Record b = new Record(topic, partition, keyBytes, valueBytes, offset, timestamp, timestampType, bHeaders);

        // THEN
        assertEquals(a, b);
        assertEquals(b, a);
    }

    @Test
    public void equalsFalseBecauseStrictSubsetTest() {
        // GIVEN
        Record a = new Record(topic, partition, keyBytes, valueBytes, offset, timestamp, timestampType, headers);
        Record b = new Record(topic, partition, keyBytes, valueBytes, offset);

        // THEN
        assertNotEquals(a, b);
        assertNotEquals(b, a);
    }

    @Test
    public void equalsFalseBecauseHeadersStrictSubsetTest() {
        // GIVEN
        ConnectHeaders aHeaders = new ConnectHeaders();
        aHeaders.add("header0-key", new SchemaAndValue(Schema.BYTES_SCHEMA, Arrays.copyOf(header0ValueBytes, header0ValueBytes.length)));
        aHeaders.add("header1-key", new SchemaAndValue(Schema.BYTES_SCHEMA, Arrays.copyOf(header1ValueBytes, header1ValueBytes.length)));
        Record a = new Record(topic, partition, keyBytes, valueBytes, offset, timestamp, timestampType, aHeaders);

        ConnectHeaders bHeaders = new ConnectHeaders();
        bHeaders.add("header0-key", new SchemaAndValue(Schema.BYTES_SCHEMA, Arrays.copyOf(header0ValueBytes, header0ValueBytes.length)));
        Record b = new Record(topic, partition, keyBytes, valueBytes, offset, timestamp, timestampType, bHeaders);

        // THEN
        assertNotEquals(a, b);
        assertNotEquals(b, a);
    }
}

