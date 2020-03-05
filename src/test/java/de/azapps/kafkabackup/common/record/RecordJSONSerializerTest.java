package de.azapps.kafkabackup.common.record;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

public class RecordJSONSerializerTest extends JSONTest {
    // Service under test:
    private static ObjectMapper sutMapper;

    @BeforeEach
    public void beforeEach() {
        sutMapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        module.addSerializer(Record.class, new RecordJSONSerializer());
        module.addSerializer(ConnectHeaders.class, new ConnectHeadersJSONSerializer());
        module.addSerializer(ConnectHeader.class, new ConnectHeaderJSONSerializer());
        sutMapper.registerModule(module);
    }

    @Test
    public void serializeTest() throws Exception {
        // GIVEN
        Record record = new Record(topic, partition, keyBytes, valueBytes, offset, timestamp, timestampType, headers);

        // WHEN
        byte[] actual = sutMapper.writeValueAsBytes(record);

        // THEN
        byte[] expected = jsonWithAllFields();
        assertArrayEquals(expected, actual);
    }

    @Test
    public void serializeTestNullKeyAndValue() throws Exception {
        // GIVEN
        Record record = new Record(topic, partition, null, null, offset, timestamp, timestampType, headers);

        // WHEN
        byte[] actual = sutMapper.writeValueAsBytes(record);

        // THEN
        byte[] expected = jsonWithNullKeyAndValue();
        assertArrayEquals(expected, actual);
    }

    @Test
    public void serializeTestNoTimestampType() throws Exception {
        // GIVEN
        Record record = new Record(topic, partition, keyBytes, valueBytes, offset, null, TimestampType.NO_TIMESTAMP_TYPE, headers);

        // WHEN
        byte[] actual = sutMapper.writeValueAsBytes(record);

        // THEN
        byte[] expected = jsonWithNoTimestampType();
        assertArrayEquals(expected, actual);
    }

    @Test
    public void serializeTestEmptyHeaders() throws Exception {
        // GIVEN
        Record record = new Record(topic, partition, keyBytes, valueBytes, offset, timestamp, timestampType);

        // WHEN
        byte[] actual = sutMapper.writeValueAsBytes(record);

        // THEN
        byte[] expected = jsonWithEmptyHeaders();
        assertArrayEquals(expected, actual);
    }
}
