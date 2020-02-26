package de.azapps.kafkabackup.common.record;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;

import static org.junit.Assert.assertEquals;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.Base64;

public class RecordJSONSerdeTest {

    private static final String topic = "test-topic";
    private static final int partition = 42;
    private static final long offset = 123;
    private static byte[] keyBytes;
    private static String keyBase64;
    private static byte[] valueBytes;
    private static String valueBase64;
    private static final String JSON_ENCODING = "UTF-8";

    @BeforeClass
    public static void setUp() throws Exception {
        // encoding here is not really important, we just want some bytes
        keyBytes = "test-key".getBytes("UTF-8");
        valueBytes = "test-value".getBytes("UTF-8");
        // using Base64 as encoding in the json is part of our Record serialization format however:
        keyBase64 = Base64.getEncoder().encodeToString(keyBytes);
        valueBase64 = Base64.getEncoder().encodeToString(valueBytes);
    }

    @Test
    public void readTest() throws Exception {
        // TODO: add timestamp, timestampType, and headers
        byte[] json = String.format("{\"topic\":\"%s\",\"partition\":%d,\"offset\":%d,\"key\":\"%s\",\"value\":\"%s\"}", topic, partition, offset, keyBase64, valueBase64).getBytes(JSON_ENCODING);
        InputStream inputStream = new ByteArrayInputStream(json);

        // WHEN
        RecordJSONSerde serde = new RecordJSONSerde();
        Record actual = serde.read(inputStream);

        // THEN
        Record expected = new Record(topic, partition, keyBytes, valueBytes, offset);
        assertEquals(expected, actual);
    }

    @Test
    public void writeTest() throws Exception {
        // GIVEN
        Record record = new Record(topic, partition, keyBytes, valueBytes, offset);

        // WHEN
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        RecordJSONSerde serde = new RecordJSONSerde();
        serde.write(outputStream, record);

        // THEN
        // NOTE: here we make some (semi-dangerous) assumptions regarding
        // - deterministic key ordering, and
        // - compact formatting without white-space
        // TODO: add timestamp, timestampType, and headers
        byte[] expected = String.format("{\"topic\":\"%s\",\"partition\":%d,\"offset\":%d,\"key\":\"%s\",\"value\":\"%s\"}", topic, partition, offset, keyBase64, valueBase64).getBytes(JSON_ENCODING);
        byte[] actual = outputStream.toByteArray();
        Assert.assertArrayEquals(expected, actual);
    }

    @Test
    public void deserializeTest() throws Exception {
        // GIVEN
        // TODO: add timestamp, timestampType, and headers
        byte[] json = String.format("{\"topic\":\"%s\",\"partition\":%d,\"offset\":%d,\"key\":\"%s\",\"value\":\"%s\"}", topic, partition, offset, keyBase64, valueBase64).getBytes(JSON_ENCODING);
        InputStream inputStream = new ByteArrayInputStream(json);

        // WHEN
        ObjectMapper mapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        module.addDeserializer(Record.class, new RecordJSONSerde.Deserializer());
        mapper.registerModule(module);
        Record actual = mapper.readValue(inputStream, Record.class);

        // THEN
        Record expected = new Record(topic, partition, keyBytes, valueBytes, offset);
        assertEquals(expected, actual);
    }

    @Test
    public void deserializeTestNullKeyAndValue() throws Exception {
        // GIVEN
        // TODO: add timestamp, timestampType, and headers
        byte[] json = String.format("{\"topic\":\"%s\",\"partition\":%d,\"offset\":%d,\"key\":null,\"value\":null}", topic, partition, offset).getBytes(JSON_ENCODING);
        InputStream inputStream = new ByteArrayInputStream(json);

        // WHEN
        ObjectMapper mapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        module.addDeserializer(Record.class, new RecordJSONSerde.Deserializer());
        mapper.registerModule(module);
        Record actual = mapper.readValue(inputStream, Record.class);

        // THEN
        Record expected = new Record(topic, partition, null, null, offset);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void serializeTest() throws Exception {
        // GIVEN
        Record record = new Record(topic, partition, keyBytes, valueBytes, offset);

        // WHEN
        ObjectMapper mapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        module.addSerializer(Record.class, new RecordJSONSerde.Serializer());
        mapper.registerModule(module);

        // THEN
        // using Base64 as encoding here is part of our Record serialization format:
        String keyBase64 = Base64.getEncoder().encodeToString(keyBytes);
        String valueBase64 = Base64.getEncoder().encodeToString(valueBytes);
        // TODO: add timestamp, timestampType, and headers
        // NOTE: here we make some (semi-dangerous) assumptions regarding
        // - deterministic key ordering, and
        // - compact formatting without white-space
        byte[] expected = String.format("{\"topic\":\"%s\",\"partition\":%d,\"offset\":%d,\"key\":\"%s\",\"value\":\"%s\"}", topic, partition, offset, keyBase64, valueBase64).getBytes(JSON_ENCODING);
        byte[] actual = mapper.writeValueAsBytes(record);
        Assert.assertArrayEquals(expected, actual);
    }

    @Test
    public void serializeTestNullKeyAndValue() throws Exception {
        // GIVEN
        Record record = new Record(topic, partition, null, null, offset);

        // WHEN
        ObjectMapper mapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        module.addSerializer(Record.class, new RecordJSONSerde.Serializer());
        mapper.registerModule(module);

        // THEN
        // TODO: add timestamp, timestampType, and headers
        // NOTE: here we make some (semi-dangerous) assumptions regarding
        // - deterministic key ordering, and
        // - compact formatting without white-space
        byte[] expected = String.format("{\"topic\":\"%s\",\"partition\":%d,\"offset\":%d,\"key\":null,\"value\":null}", topic, partition, offset).getBytes(JSON_ENCODING);
        byte[] actual = mapper.writeValueAsBytes(record);
        Assert.assertArrayEquals(expected, actual);
    }

}

