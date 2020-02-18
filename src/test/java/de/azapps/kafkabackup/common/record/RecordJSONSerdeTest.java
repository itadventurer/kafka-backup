package de.azapps.kafkabackup.common.record;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import de.azapps.kafkabackup.common.record.Record;

import static org.junit.Assert.assertEquals;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.Files;
import java.util.Base64;
import java.util.Comparator;

public class RecordJSONSerdeTest {

    @Test
    public void readTest() throws Exception {
        // GIVEN
        String topic = "test-topic";
        int partition = 42;
        long offset = 123;
        // encoding here is not really important, we just want some bytes
        byte[] key = "test-key".getBytes("UTF-8");
        byte[] value = "test-value".getBytes("UTF-8");
        // using Base64 as encoding here is part of our Record serialization format however:
        String keyBase64 = Base64.getEncoder().encodeToString(key);
        String valueBase64 = Base64.getEncoder().encodeToString(value);
        // TODO: add timestamp, timestampType, and headers
        byte[] json = String.format("{\"topic\":\"%s\",\"partition\":%d,\"offset\":%d,\"key\":\"%s\",\"value\":\"%s\"}", topic, partition, offset, keyBase64, valueBase64).getBytes("UTF-8");
        InputStream inputStream = new ByteArrayInputStream(json);

        // WHEN
        RecordJSONSerde serde = new RecordJSONSerde();
        Record actual = serde.read(inputStream);

        // THEN
        Record expected = new Record(topic, partition, key, value, offset);
        assertEquals(expected, actual);
    }

    @Test
    public void writeTest() throws Exception {
        // GIVEN
        String topic = "test-topic";
        int partition = 42;
        long offset = 123;
        // encoding here is not really important, we just want some bytes
        byte[] key = "test-key".getBytes("UTF-8");
        byte[] value = "test-value".getBytes("UTF-8");
        Record record = new Record(topic, partition, key, value, offset);

        // WHEN
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        RecordJSONSerde serde = new RecordJSONSerde();
        serde.write(outputStream, record);

        // THEN
        // using Base64 as encoding here is part of our Record serialization format:
        String keyBase64 = Base64.getEncoder().encodeToString(key);
        String valueBase64 = Base64.getEncoder().encodeToString(value);
        // TODO: add timestamp, timestampType, and headers
        // NOTE: here we make some (semi-dangerous) assumptions regarding
        // - deterministic key ordering, and
        // - compact formatting without white-space
        byte[] expected = String.format("{\"topic\":\"%s\",\"partition\":%d,\"offset\":%d,\"key\":\"%s\",\"value\":\"%s\"}", topic, partition, offset, keyBase64, valueBase64).getBytes("UTF-8");
        byte[] actual = outputStream.toByteArray();
        Assert.assertArrayEquals(expected, actual);
    }

    @Test
    public void deserializeTest() throws Exception {
        // GIVEN
        String topic = "test-topic";
        int partition = 42;
        long offset = 123;
        // encoding here is not really important, we just want some bytes
        byte[] key = "test-key".getBytes("UTF-8");
        byte[] value = "test-value".getBytes("UTF-8");
        // using Base64 as encoding here is part of our Record serialization format however:
        String keyBase64 = Base64.getEncoder().encodeToString(key);
        String valueBase64 = Base64.getEncoder().encodeToString(value);
        // TODO: add timestamp, timestampType, and headers
        byte[] json = String.format("{\"topic\":\"%s\",\"partition\":%d,\"offset\":%d,\"key\":\"%s\",\"value\":\"%s\"}", topic, partition, offset, keyBase64, valueBase64).getBytes("UTF-8");
        InputStream inputStream = new ByteArrayInputStream(json);

        // WHEN
        ObjectMapper mapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        module.addDeserializer(Record.class, new RecordJSONSerde.Deserializer());
        mapper.registerModule(module);
        Record actual = mapper.readValue(inputStream, Record.class);

        // THEN
        Record expected = new Record(topic, partition, key, value, offset);
        assertEquals(expected, actual);
    }

    @Test
    public void deserializeTestNullKeyAndValue() throws Exception {
        // GIVEN
        String topic = "test-topic";
        int partition = 42;
        long offset = 123;
        // TODO: add timestamp, timestampType, and headers
        byte[] json = String.format("{\"topic\":\"%s\",\"partition\":%d,\"offset\":%d,\"key\":null,\"value\":null}", topic, partition, offset).getBytes("UTF-8");
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
        String topic = "test-topic";
        int partition = 42;
        long offset = 123;
        // encoding here is not really important, we just want some bytes
        byte[] key = "test-key".getBytes("UTF-8");
        byte[] value = "test-value".getBytes("UTF-8");
        Record record = new Record(topic, partition, key, value, offset);

        // WHEN
        ObjectMapper mapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        module.addSerializer(Record.class, new RecordJSONSerde.Serializer());
        mapper.registerModule(module);

        // THEN
        // using Base64 as encoding here is part of our Record serialization format:
        String keyBase64 = Base64.getEncoder().encodeToString(key);
        String valueBase64 = Base64.getEncoder().encodeToString(value);
        // TODO: add timestamp, timestampType, and headers
        // NOTE: here we make some (semi-dangerous) assumptions regarding
        // - deterministic key ordering, and
        // - compact formatting without white-space
        byte[] expected = String.format("{\"topic\":\"%s\",\"partition\":%d,\"offset\":%d,\"key\":\"%s\",\"value\":\"%s\"}", topic, partition, offset, keyBase64, valueBase64).getBytes("UTF-8");
        byte[] actual = mapper.writeValueAsBytes(record);
        Assert.assertArrayEquals(expected, actual);
    }

    @Test
    public void serializeTestNullKeyAndValue() throws Exception {
        // GIVEN
        String topic = "test-topic";
        int partition = 42;
        long offset = 123;
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
        byte[] expected = String.format("{\"topic\":\"%s\",\"partition\":%d,\"offset\":%d,\"key\":null,\"value\":null}", topic, partition, offset).getBytes("UTF-8");
        byte[] actual = mapper.writeValueAsBytes(record);
        Assert.assertArrayEquals(expected, actual);
    }
}

