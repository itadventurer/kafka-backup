package de.azapps.kafkabackup.common.record;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;

import static org.junit.Assert.assertEquals;

import org.junit.Assert;
import org.junit.Test;
import java.util.Base64;

public class RecordJSONSerializerTest {

    @Test
    public void serializeTest() throws Exception {
        // Prepare input
        String topic = "test-topic";
        int partition = 42;
        long offset = 123;
        // encoding here is not really important, we just want some bytes
        byte[] key = "test-key".getBytes("UTF-8");
        byte[] value = "test-value".getBytes("UTF-8");
        Record record = new Record(topic, partition, key, value, offset);

        // Prepare expectations
        // using Base64 as encoding here is part of our Record serialization format:
        String keyBase64 = Base64.getEncoder().encodeToString(key);
        String valueBase64 = Base64.getEncoder().encodeToString(value);
        // TODO: add timestamp, timestampType, and headers
        // NOTE: here we make some (semi-dangerous) assumptions regarding
        // - deterministic key ordering, and
        // - compact formatting without white-space
        byte[] expected = String.format("{\"topic\":\"%s\",\"partition\":%d,\"offset\":%d,\"key\":\"%s\",\"value\":\"%s\"}", topic, partition, offset, keyBase64, valueBase64).getBytes("UTF-8");

        // Test
        ObjectMapper mapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        module.addSerializer(Record.class, new RecordJSONSerializer());
        mapper.registerModule(module);
        byte[] actual = mapper.writeValueAsBytes(record);

        // Do assertions
        Assert.assertArrayEquals(expected, actual);
    }
}

