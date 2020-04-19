package de.azapps.kafkabackup.common.record;

import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.sink.SinkRecord;
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
    private static final RecordHeaders HEADERS = new RecordHeaders();

    static {
        HEADERS.add("", new byte[0]);
        HEADERS.add("null", null);
        HEADERS.add("value0", HEADER_0_VALUE_BYTES);
        HEADERS.add("value1", HEADER_1_VALUE_BYTES);
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

        RecordHeaders bHeaders = new RecordHeaders();
        bHeaders.add("", new byte[0]);
        bHeaders.add("null", null);
        bHeaders.add("value0", Arrays.copyOf(HEADER_0_VALUE_BYTES, HEADER_0_VALUE_BYTES.length));
        bHeaders.add("value1", Arrays.copyOf(HEADER_1_VALUE_BYTES, HEADER_1_VALUE_BYTES.length));
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
        RecordHeaders aHeaders = new RecordHeaders();
        aHeaders.add("header0-key", Arrays.copyOf(HEADER_0_VALUE_BYTES, HEADER_0_VALUE_BYTES.length));
        aHeaders.add("header1-key", Arrays.copyOf(HEADER_1_VALUE_BYTES, HEADER_1_VALUE_BYTES.length));
        Record a = new Record(TOPIC, PARTITION, KEY_BYTES, VALUE_BYTES, OFFSET, TIMESTAMP, TIMESTAMP_TYPE, aHeaders);

        RecordHeaders bHeaders = new RecordHeaders();
        bHeaders.add("header0-key", Arrays.copyOf(HEADER_0_VALUE_BYTES, HEADER_0_VALUE_BYTES.length));
        Record b = new Record(TOPIC, PARTITION, KEY_BYTES, VALUE_BYTES, OFFSET, TIMESTAMP, TIMESTAMP_TYPE, bHeaders);


        RecordHeaders cHeaders = new RecordHeaders();
        cHeaders.add("header1-key", Arrays.copyOf(HEADER_0_VALUE_BYTES, HEADER_0_VALUE_BYTES.length));
        cHeaders.add("header1-key", Arrays.copyOf(HEADER_1_VALUE_BYTES, HEADER_1_VALUE_BYTES.length));
        Record c = new Record(TOPIC, PARTITION, KEY_BYTES, VALUE_BYTES, OFFSET, TIMESTAMP, TIMESTAMP_TYPE, cHeaders);

        // THEN
        assertNotEquals(a, b);
        assertNotEquals(b, a);
        assertNotEquals(a, c);
        assertNotEquals(b, c);
    }

    /**
     * This is not used during normal operations, but we need to verify that this works
     * correctly as we use the functions for our end to end tests!
     */
    @Test
    public void roundtripSinkRecordTest() {

        // given
        Record a = new Record(TOPIC, PARTITION, KEY_BYTES, VALUE_BYTES, OFFSET, TIMESTAMP, TIMESTAMP_TYPE, HEADERS);
        Record b = new Record(TOPIC, PARTITION, KEY_BYTES, VALUE_BYTES, 3, null, TimestampType.NO_TIMESTAMP_TYPE, HEADERS);
        Record c = new Record(TOPIC, PARTITION, KEY_BYTES, VALUE_BYTES, 0);
        Record d = new Record(TOPIC, PARTITION, null, null, 1);
        Record e = new Record(TOPIC, PARTITION, new byte[0], new byte[0], 2);
        Record f = new Record(TOPIC, PARTITION, KEY_BYTES, VALUE_BYTES, OFFSET);

        // transform
        SinkRecord srA = a.toSinkRecord();
        SinkRecord srB = b.toSinkRecord();
        SinkRecord srC = c.toSinkRecord();
        SinkRecord srD = d.toSinkRecord();
        SinkRecord srE = e.toSinkRecord();
        SinkRecord srF = f.toSinkRecord();

        // expect
        assertEquals(a, Record.fromSinkRecord(srA));
        assertEquals(b, Record.fromSinkRecord(srB));
        assertEquals(c, Record.fromSinkRecord(srC));
        assertEquals(d, Record.fromSinkRecord(srD));
        assertEquals(e, Record.fromSinkRecord(srE));
        assertEquals(f, Record.fromSinkRecord(srF));


    }
}
