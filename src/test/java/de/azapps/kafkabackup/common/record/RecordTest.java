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
    private static final ConnectHeader header0 = new ConnectHeader(header0Key, new SchemaAndValue(Schema.OPTIONAL_BYTES_SCHEMA, header0ValueBytes));
    private static final ConnectHeader header1 = new ConnectHeader(header1Key, new SchemaAndValue(Schema.OPTIONAL_BYTES_SCHEMA, header1ValueBytes));
    private static final ConnectHeader header2 = new ConnectHeader(header2Key, new SchemaAndValue(Schema.OPTIONAL_BYTES_SCHEMA, header2ValueBytes));
    private static final ConnectHeaders headers = new ConnectHeaders(Arrays.asList((new ConnectHeader[]{header0, header1, header2})));

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
        ConnectHeader aHeader0 = new ConnectHeader("header0-key", new SchemaAndValue(Schema.BYTES_SCHEMA, Arrays.copyOf(header0ValueBytes, header0ValueBytes.length)));
        ConnectHeader aHeader1 = new ConnectHeader("header1-key", new SchemaAndValue(Schema.BYTES_SCHEMA, Arrays.copyOf(header1ValueBytes, header1ValueBytes.length)));
        ConnectHeader aHeader2 = new ConnectHeader("header2-key", new SchemaAndValue(Schema.OPTIONAL_BYTES_SCHEMA, null));
        ConnectHeaders aHeaders = new ConnectHeaders(Arrays.asList((new ConnectHeader[]{aHeader0, aHeader1, aHeader2})));
        Record a = new Record(topic, partition, keyBytes, valueBytes, offset, timestamp, timestampType, aHeaders);

        ConnectHeader bHeader0 = new ConnectHeader("header0-key", new SchemaAndValue(Schema.BYTES_SCHEMA, Arrays.copyOf(header0ValueBytes, header0ValueBytes.length)));
        ConnectHeader bHeader1 = new ConnectHeader("header1-key", new SchemaAndValue(Schema.BYTES_SCHEMA, Arrays.copyOf(header1ValueBytes, header1ValueBytes.length)));
        ConnectHeader bHeader2 = new ConnectHeader("header2-key", new SchemaAndValue(Schema.OPTIONAL_BYTES_SCHEMA, null));
        ConnectHeaders bHeaders = new ConnectHeaders(Arrays.asList((new ConnectHeader[]{bHeader0, bHeader1, bHeader2})));
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
        Record a = new Record(topic, partition, keyBytes, valueBytes, offset, timestamp, timestampType, headers);

        ConnectHeaders bHeaders = new ConnectHeaders(Arrays.asList((new ConnectHeader[]{header0, header1})));
        Record b = new Record(topic, partition, keyBytes, valueBytes, offset, timestamp, timestampType, bHeaders);

        // THEN
        assertNotEquals(a, b);
        assertNotEquals(b, a);
    }
}

