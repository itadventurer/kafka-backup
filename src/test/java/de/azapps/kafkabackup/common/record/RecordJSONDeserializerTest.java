package de.azapps.kafkabackup.common.record;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import de.azapps.kafkabackup.common.record.Record;

import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.Files;
import java.util.Base64;
import java.util.Comparator;

public class RecordJSONDeserializerTest {

    @Test
    public void deserializeTest() throws Exception {
        // Prepare input
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

        // Prepare expectations
        Record expected = new Record(topic, partition, key, value, offset);

        // Test
        ObjectMapper mapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        module.addDeserializer(Record.class, new RecordJSONDeserializer());
        mapper.registerModule(module);
        Record actual = mapper.readValue(inputStream, Record.class);

        // Do assertions
        assertEquals(expected, actual);
    }
}
