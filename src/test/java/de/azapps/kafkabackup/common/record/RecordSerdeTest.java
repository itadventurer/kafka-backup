package de.azapps.kafkabackup.common.record;

import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class RecordSerdeTest {

    private static final String TOPIC = "test-topic";
    private static final int PARTITION = 42;
    private static final long OFFSET = 123;
    private static final byte[] KEY_BYTES = "test-key".getBytes(StandardCharsets.UTF_8);
    private static final byte[] VALUE_BYTES = "test-value".getBytes(StandardCharsets.UTF_8);
    private static final byte[] NULL_TIMESTAMP_BYTES = "null-timestamp".getBytes(StandardCharsets.UTF_8);

    private static final String SIMPLE_RECORD_FILE = "simple_record";
    private static final String NULL_RECORD_FILE = "null_record";
    private static final String EMPTY_RECORD_FILE = "empty_record";
    private static final String HEADER_RECORD_FILE = "header_record";

    // Example records
    private static final Record SIMPLE_RECORD, NULL_RECORD, EMPTY_RECORD, HEADER_RECORD, NULL_TIMESTAMP_RECORD;

    private static final byte[] NON_UTF8_BYTES = {0x012, 0x00, 0xf};

    static {
        SIMPLE_RECORD = new Record(TOPIC, PARTITION, KEY_BYTES, VALUE_BYTES, OFFSET);
        NULL_RECORD = new Record(TOPIC, PARTITION, null, null, OFFSET);
        EMPTY_RECORD = new Record(TOPIC, PARTITION, new byte[0], new byte[0], OFFSET);
        NULL_TIMESTAMP_RECORD = new Record(TOPIC, PARTITION, NULL_TIMESTAMP_BYTES, null, OFFSET, null, TimestampType.CREATE_TIME);
        // Build multiple headers that might cause problems
        RecordHeaders headers = new RecordHeaders();
        headers.add("", new byte[0]);
        headers.add("null", null);
        headers.add("value", VALUE_BYTES);
        HEADER_RECORD = new Record(TOPIC, PARTITION, KEY_BYTES, VALUE_BYTES, OFFSET, null, TimestampType.NO_TIMESTAMP_TYPE, headers);
    }

    @Test
    public void roundtripTest() throws Exception {
        Record simpleRoundtrip = writeAndReadRecord(SIMPLE_RECORD);
        assertEquals(SIMPLE_RECORD, simpleRoundtrip);

        // non-utf8
        RecordHeaders headers = new RecordHeaders();
        headers.add("nonutf8", NON_UTF8_BYTES);
        Record nonUtf8Headers = new Record(TOPIC, PARTITION, KEY_BYTES, VALUE_BYTES, OFFSET, null, TimestampType.NO_TIMESTAMP_TYPE, headers);
        Record nonUtf8Roundtrip = writeAndReadRecord(nonUtf8Headers);
        assertEquals(nonUtf8Headers, nonUtf8Roundtrip);

    }

    @Test
    public void roundtripWithNull() throws Exception {
        Record nullRoundtrip = writeAndReadRecord(NULL_RECORD);
        assertEquals(NULL_RECORD, nullRoundtrip);

        Record emptyRoundtrip = writeAndReadRecord(EMPTY_RECORD);
        assertEquals(EMPTY_RECORD, emptyRoundtrip);

        // Must be different
        assertNotEquals(nullRoundtrip, emptyRoundtrip);
    }

    @Test
    public void roundtripNullTimestamp() throws Exception {
        Record nullTimestampRoundtrip =writeAndReadRecord(NULL_TIMESTAMP_RECORD);
        assertEquals(NULL_TIMESTAMP_RECORD, nullTimestampRoundtrip);
    }

    @Test
    public void roundtripHeaders() throws Exception {
        Record headerRoundtrip = writeAndReadRecord(HEADER_RECORD);
        assertEquals(HEADER_RECORD, headerRoundtrip);
    }

    /**
     * DO NOT CHANGE THIS TEST!
     */
    @Test
    public void readV1() throws Exception {
        File v1Directory = new File("src/test/assets/v1/records");
        Record simpleRecord = readFromFile(new File(v1Directory, SIMPLE_RECORD_FILE));
        assertEquals(SIMPLE_RECORD, simpleRecord);
        Record nullRecord = readFromFile(new File(v1Directory, NULL_RECORD_FILE));
        assertEquals(NULL_RECORD, nullRecord);
        assertNotEquals(SIMPLE_RECORD, nullRecord); // just to make sure!
        Record emptyRecord = readFromFile(new File(v1Directory, EMPTY_RECORD_FILE));
        assertEquals(EMPTY_RECORD, emptyRecord);
        assertNotEquals(NULL_RECORD, emptyRecord); // just to make sure!
        Record headerRecord = readFromFile(new File(v1Directory, HEADER_RECORD_FILE));
        assertEquals(HEADER_RECORD, headerRecord);
        assertNotEquals(EMPTY_RECORD, headerRecord); // just to make sure!
    }

    // UTILS

    private Record writeAndReadRecord(Record record) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        RecordSerde.write(outputStream, record);
        byte[] data = outputStream.toByteArray();
        return RecordSerde.read(TOPIC, PARTITION, new ByteArrayInputStream(data));
    }

    private static Record readFromFile(File file) throws IOException {
        FileInputStream inputStream = new FileInputStream(file);
        return RecordSerde.read(TOPIC, PARTITION, inputStream);
    }

    /**
     * Utility function to be run once when the format on disk changes to be able to stay backwards-compatible
     * <p>
     * Call it manually once when the format changes
     */
    private static void writeTestRecordsToFile() throws IOException {
        File directory = new File("src/test/assets/v1_nonutf8/records"); // CHANGEME WHEN CHANGING DATA FORMAT!
        writeCurrentVersionRecordToFile(SIMPLE_RECORD, new File(directory, SIMPLE_RECORD_FILE));
        writeCurrentVersionRecordToFile(NULL_RECORD, new File(directory, NULL_RECORD_FILE));
        writeCurrentVersionRecordToFile(EMPTY_RECORD, new File(directory, EMPTY_RECORD_FILE));
        writeCurrentVersionRecordToFile(HEADER_RECORD, new File(directory, HEADER_RECORD_FILE));
    }

    private static void writeCurrentVersionRecordToFile(Record record, File file) throws IOException {
        FileOutputStream outputStream = new FileOutputStream(file);
        RecordSerde.write(outputStream, record);
    }
}
