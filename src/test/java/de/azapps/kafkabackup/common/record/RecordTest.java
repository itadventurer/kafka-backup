package de.azapps.kafkabackup.common.record;

import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class RecordTest {
    private static final String TOPIC = "test-topic";
    private static final int PARTITION = 42;
    private static final long OFFSET = 123;
    private static final TimestampType TIMESTAMP_TYPE = TimestampType.LOG_APPEND_TIME;
    private static final Long TIMESTAMP = 573831430000L;
    // encoding here is not really important, we just want some bytes
    private static final byte[] KEY_BYTES = "test-key".getBytes(StandardCharsets.UTF_8);
    private static final byte[] VALUE_BYTES = "test-value".getBytes(StandardCharsets.UTF_8);
    // Header fixtures:
    private static final byte[] HEADER_0_VALUE_BYTES = "header0-value".getBytes(StandardCharsets.UTF_8);
    private static final byte[] HEADER_1_VALUE_BYTES = "header1-value".getBytes(StandardCharsets.UTF_8);
    private static final ConnectHeaders HEADERS = new ConnectHeaders();

    static {
        HEADERS.add("", new SchemaAndValue(Schema.OPTIONAL_BYTES_SCHEMA, new byte[0]));
        HEADERS.add("null", new SchemaAndValue(Schema.OPTIONAL_BYTES_SCHEMA, null));
        HEADERS.add("value0", new SchemaAndValue(Schema.OPTIONAL_BYTES_SCHEMA, HEADER_0_VALUE_BYTES));
        HEADERS.add("value1", new SchemaAndValue(Schema.OPTIONAL_BYTES_SCHEMA, HEADER_1_VALUE_BYTES));
    }

    @Test
    public void equalsIdentityTrueTest() {
        // GIVEN
        Record a = new Record(TOPIC, PARTITION, KEY_BYTES, VALUE_BYTES, OFFSET, TIMESTAMP, TIMESTAMP_TYPE, HEADERS);

        // THEN
        assertEquals(a, a);
    }

    @Test
    public void equalsValueTrueTest() {
        // GIVEN
        Record a = new Record(TOPIC, PARTITION, KEY_BYTES, VALUE_BYTES, OFFSET, TIMESTAMP, TIMESTAMP_TYPE, HEADERS);

        ConnectHeaders bHeaders = new ConnectHeaders();
        bHeaders.add("", new SchemaAndValue(Schema.OPTIONAL_BYTES_SCHEMA, new byte[0]));
        bHeaders.add("null", new SchemaAndValue(Schema.OPTIONAL_BYTES_SCHEMA, null));
        bHeaders.add("value0", new SchemaAndValue(Schema.OPTIONAL_BYTES_SCHEMA, Arrays.copyOf(HEADER_0_VALUE_BYTES, HEADER_0_VALUE_BYTES.length)));
        bHeaders.add("value1", new SchemaAndValue(Schema.OPTIONAL_BYTES_SCHEMA, Arrays.copyOf(HEADER_1_VALUE_BYTES, HEADER_1_VALUE_BYTES.length)));
        Record b = new Record(TOPIC, PARTITION, KEY_BYTES, VALUE_BYTES, OFFSET, TIMESTAMP, TIMESTAMP_TYPE, bHeaders);

        // THEN
        assertEquals(a, b);
        assertEquals(b, a);
    }

    @Test
    public void equalsFalseBecauseStrictSubsetTest() {
        // GIVEN
        Record a = new Record(TOPIC, PARTITION, KEY_BYTES, VALUE_BYTES, OFFSET, TIMESTAMP, TIMESTAMP_TYPE, HEADERS);
        Record b = new Record(TOPIC, PARTITION, KEY_BYTES, VALUE_BYTES, OFFSET);

        // THEN
        assertNotEquals(a, b);
        assertNotEquals(b, a);
    }

    @Test
    public void equalsFalseBecauseHeadersStrictSubsetTest() {
        // GIVEN
        ConnectHeaders aHeaders = new ConnectHeaders();
        aHeaders.add("header0-key", new SchemaAndValue(Schema.OPTIONAL_BYTES_SCHEMA, Arrays.copyOf(HEADER_0_VALUE_BYTES, HEADER_0_VALUE_BYTES.length)));
        aHeaders.add("header1-key", new SchemaAndValue(Schema.OPTIONAL_BYTES_SCHEMA, Arrays.copyOf(HEADER_1_VALUE_BYTES, HEADER_1_VALUE_BYTES.length)));
        Record a = new Record(TOPIC, PARTITION, KEY_BYTES, VALUE_BYTES, OFFSET, TIMESTAMP, TIMESTAMP_TYPE, aHeaders);

        ConnectHeaders bHeaders = new ConnectHeaders();
        bHeaders.add("header0-key", new SchemaAndValue(Schema.OPTIONAL_BYTES_SCHEMA, Arrays.copyOf(HEADER_0_VALUE_BYTES, HEADER_0_VALUE_BYTES.length)));
        Record b = new Record(TOPIC, PARTITION, KEY_BYTES, VALUE_BYTES, OFFSET, TIMESTAMP, TIMESTAMP_TYPE, bHeaders);


        ConnectHeaders cHeaders = new ConnectHeaders();
        cHeaders.add("header1-key", new SchemaAndValue(Schema.OPTIONAL_BYTES_SCHEMA, Arrays.copyOf(HEADER_0_VALUE_BYTES, HEADER_0_VALUE_BYTES.length)));
        cHeaders.add("header1-key", new SchemaAndValue(Schema.OPTIONAL_BYTES_SCHEMA, Arrays.copyOf(HEADER_1_VALUE_BYTES, HEADER_1_VALUE_BYTES.length)));
        Record c = new Record(TOPIC, PARTITION, KEY_BYTES, VALUE_BYTES, OFFSET, TIMESTAMP, TIMESTAMP_TYPE, cHeaders);

        // THEN
        assertNotEquals(a, b);
        assertNotEquals(b, a);
        assertNotEquals(a, c);
        assertNotEquals(b, c);
    }
}
