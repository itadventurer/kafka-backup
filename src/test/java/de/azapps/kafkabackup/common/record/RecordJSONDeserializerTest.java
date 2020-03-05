package de.azapps.kafkabackup.common.record;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RecordJSONDeserializerTest extends JSONTest {
    // Service under test:
    private static ObjectMapper sutMapper;

    @BeforeEach
    public void beforeEach() {
        sutMapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        module.addDeserializer(Record.class, new RecordJSONDeserializer());
        module.addDeserializer(ConnectHeaders.class, new ConnectHeadersJSONDeserializer());
        sutMapper.registerModule(module);
    }

    @Test
    public void deserializeTest() throws Exception {
        // GIVEN
        InputStream inputStream = new ByteArrayInputStream(jsonWithAllFields());

        // WHEN
        Record actual = sutMapper.readValue(inputStream, Record.class);

        // THEN
        Record expected = new Record(topic, partition, keyBytes, valueBytes, offset, timestamp, timestampType, headers);
        assertEquals(expected, actual);
    }

    @Test
    public void deserializeTestNullKeyAndValue() throws Exception {
        // GIVEN
        InputStream inputStream = new ByteArrayInputStream(jsonWithNullKeyAndValue());

        // WHEN
        Record actual = sutMapper.readValue(inputStream, Record.class);

        // THEN
        Record expected = new Record(topic, partition, null, null, offset, timestamp, timestampType, headers);
        assertEquals(expected, actual);
    }

    @Test
    public void deserializeTestNoTimestampType() throws Exception {
        // GIVEN
        InputStream inputStream = new ByteArrayInputStream(jsonWithNoTimestampType());

        // WHEN
        Record actual = sutMapper.readValue(inputStream, Record.class);

        // THEN
        Record expected = new Record(topic, partition, keyBytes, valueBytes, offset, null, TimestampType.NO_TIMESTAMP_TYPE, headers);
        assertEquals(expected, actual);
    }

    @Test
    public void deserializeTestEmptyHeaders() throws Exception {
        // GIVEN
        InputStream inputStream = new ByteArrayInputStream(jsonWithEmptyHeaders());

        // WHEN
        Record actual = sutMapper.readValue(inputStream, Record.class);

        // THEN
        Record expected = new Record(topic, partition, keyBytes, valueBytes, offset, timestamp, timestampType);
        assertEquals(expected, actual);
    }

    @Test
    public void deserializeTestNoHeaders() throws Exception {
        // GIVEN
        InputStream inputStream = new ByteArrayInputStream(jsonWithNoHeaders());

        // WHEN
        Record actual = sutMapper.readValue(inputStream, Record.class);

        // THEN
        Record expected = new Record(topic, partition, keyBytes, valueBytes, offset, timestamp, timestampType);
        assertEquals(expected, actual);
    }
}
