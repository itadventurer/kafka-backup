package de.azapps.kafkabackup.common.record;

import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.header.Header;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class RecordSerdeTest {


    private static final String topic = "test-topic";
    private static final int partition = 42;
    private static final long offset = 123;
    private static byte[] keyBytes;
    private static String keyBase64;
    private static byte[] valueBytes;
    private static String valueBase64;

    @BeforeAll
    public static void setUp() throws Exception {
        // encoding here is not really important, we just want some bytes
        keyBytes = "test-key".getBytes(StandardCharsets.UTF_8);
        valueBytes = "test-value".getBytes(StandardCharsets.UTF_8);
        // using Base64 as encoding in the json is part of our Record serialization format however:
        keyBase64 = Base64.getEncoder().encodeToString(keyBytes);
        valueBase64 = Base64.getEncoder().encodeToString(valueBytes);
    }

    @Test
    public void roundtripTest() throws Exception {
        Record record = new Record(topic, partition, keyBytes, valueBytes, offset);

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        RecordSerde.write(outputStream, record);
        byte[] data = outputStream.toByteArray();
        Record toCheck = RecordSerde.read(topic, partition, new ByteArrayInputStream(data));
        assertEquals(record, toCheck);
    }

    @Test
    public void roundtripWithNull() throws Exception {
        // NULL Record
        Record nullRecord = new Record(topic, partition, null, null, offset);
        ByteArrayOutputStream outputStreamNull = new ByteArrayOutputStream();
        RecordSerde.write(outputStreamNull, nullRecord);
        byte[] data = outputStreamNull.toByteArray();
        Record toCheckNull = RecordSerde.read(topic, partition, new ByteArrayInputStream(data));
        assertEquals(nullRecord, toCheckNull);

        // EmptyRecord
        Record emptyRecord = new Record(topic, partition, new byte[0], new byte[0], offset);
        ByteArrayOutputStream outputStreamEmpty = new ByteArrayOutputStream();
        RecordSerde.write(outputStreamEmpty, emptyRecord);
        byte[] dataEmpty = outputStreamEmpty.toByteArray();
        Record toCheckEmpty = RecordSerde.read(topic, partition, new ByteArrayInputStream(dataEmpty));
        assertEquals(emptyRecord, toCheckEmpty);

        // Must be different
        assertNotEquals(toCheckEmpty, toCheckNull);
    }

    @Test
    public void roundtripHeaders() throws Exception {
        // Build multiple headers that might cause problems
        List<Header> headers = new ArrayList<>();
        headers.add(new ConnectHeader("", new SchemaAndValue(Schema.BYTES_SCHEMA, new byte[0])));
        headers.add(new ConnectHeader("null", new SchemaAndValue(Schema.BYTES_SCHEMA, null)));
        headers.add(new ConnectHeader("value", new SchemaAndValue(Schema.BYTES_SCHEMA, valueBytes)));

        Record record = new Record(topic, partition, keyBytes, valueBytes, offset, null, TimestampType.NO_TIMESTAMP_TYPE, headers);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        RecordSerde.write(outputStream, record);
        byte[] data = outputStream.toByteArray();
        Record toCheck = RecordSerde.read(topic, partition, new ByteArrayInputStream(data));
        assertEquals(record, toCheck);
    }

}
