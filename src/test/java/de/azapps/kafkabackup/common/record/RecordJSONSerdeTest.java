package de.azapps.kafkabackup.common.record;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;

import org.apache.kafka.common.record.TimestampType;

public class RecordJSONSerdeTest {

    private static final String topic = "test-topic";
    private static final int partition = 42;
    private static final long offset = 123;
    private static final TimestampType timestampType = TimestampType.LOG_APPEND_TIME;
    private static final Long timestamp = 573831430000L;
    // encoding here is not really important, we just want some bytes
    private static final byte[] keyBytes = "test-key".getBytes(StandardCharsets.UTF_8);
    private static final byte[] valueBytes = "test-value".getBytes(StandardCharsets.UTF_8);
    // using Base64 as encoding in the json is part of our Record serialization format however:
    private static final String keyBase64 = Base64.getEncoder().encodeToString(keyBytes);
    private static final String valueBase64 = Base64.getEncoder().encodeToString(valueBytes);
    // Header fixtures:
    private static final String header0Key = "header0-key";
    private static final String header1Key = "header1-key";
    private static final String header2Key = "header2-key";
    private static final byte[] header0ValueBytes = "header0-value".getBytes(StandardCharsets.UTF_8);
    private static final byte[] header1ValueBytes = "header1-value".getBytes(StandardCharsets.UTF_8);
    private static final byte[] header2ValueBytes = null;
    private static final String header0ValueBase64 = Base64.getEncoder().encodeToString(header0ValueBytes);
    private static final String header1ValueBase64 = Base64.getEncoder().encodeToString(header1ValueBytes);
    // header2Value has no base64 encoding
    private static final ConnectHeader header0 = new ConnectHeader(header0Key, new SchemaAndValue(Schema.OPTIONAL_BYTES_SCHEMA, header0ValueBytes));
    private static final ConnectHeader header1 = new ConnectHeader(header1Key, new SchemaAndValue(Schema.OPTIONAL_BYTES_SCHEMA, header1ValueBytes));
    private static final ConnectHeader header2 = new ConnectHeader(header2Key, new SchemaAndValue(Schema.OPTIONAL_BYTES_SCHEMA, header2ValueBytes));
    private static final ConnectHeaders headers = new ConnectHeaders(Arrays.asList((new ConnectHeader[]{header0, header1, header2})));

    private static final Charset JSON_ENCODING = StandardCharsets.UTF_8;
    // Services under test:
    private static ObjectMapper sutMapper;
    private static RecordJSONSerde sutSerde;

    @BeforeEach
    public void beforeEach() {
        sutMapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        module.addDeserializer(Record.class, new RecordJSONSerde.RecordDeserializer());
        module.addDeserializer(ConnectHeaders.class, new RecordJSONSerde.HeadersDeserializer());
        module.addDeserializer(ConnectHeader.class, new RecordJSONSerde.HeaderDeserializer());
        module.addSerializer(Record.class, new RecordJSONSerde.RecordSerializer());
        module.addSerializer(ConnectHeaders.class, new RecordJSONSerde.HeadersSerializer());
        module.addSerializer(ConnectHeader.class, new RecordJSONSerde.HeaderSerializer());
        sutMapper.registerModule(module);

        sutSerde = new RecordJSONSerde();
    }

    private String jsonHeadersStr() {
        return "["+
                    String.format("{\"key\":\"%s\",\"value\":\"%s\"},",header0Key, header0ValueBase64)+
                    String.format("{\"key\":\"%s\",\"value\":\"%s\"},",header1Key, header1ValueBase64)+
                    String.format("{\"key\":\"%s\",\"value\":%s}",header2Key, "null")+
                "]";
    }

    private byte[] jsonWithAllFields() {
        return ("{"+
                    String.format("\"topic\":\"%s\",", topic)+
                    String.format("\"partition\":%d,", partition)+
                    String.format("\"offset\":%d,", offset)+
                    String.format("\"timestamp_type\":\"%s\",", timestampType)+
                    String.format("\"timestamp\":%d,", timestamp)+
                    String.format("\"key\":\"%s\",", keyBase64)+
                    String.format("\"value\":\"%s\",", valueBase64)+
                    String.format("\"headers\":%s", jsonHeadersStr())+
                "}").getBytes(JSON_ENCODING);
    }

    private byte[] jsonWithNullKeyAndValue() {
        return ("{"+
                String.format("\"topic\":\"%s\",", topic)+
                String.format("\"partition\":%d,", partition)+
                String.format("\"offset\":%d,", offset)+
                String.format("\"timestamp_type\":\"%s\",", timestampType)+
                String.format("\"timestamp\":%d,", timestamp)+
                String.format("\"key\":%s,", "null")+
                String.format("\"value\":%s,", "null")+
                String.format("\"headers\":%s", jsonHeadersStr())+
                "}").getBytes(JSON_ENCODING);
    }

    private byte[] jsonWithNoTimestampType() {
        return ("{"+
                String.format("\"topic\":\"%s\",", topic)+
                String.format("\"partition\":%d,", partition)+
                String.format("\"offset\":%d,", offset)+
                String.format("\"timestamp_type\":\"%s\",", TimestampType.NO_TIMESTAMP_TYPE)+
                String.format("\"timestamp\":%s,", "null")+
                String.format("\"key\":\"%s\",", keyBase64)+
                String.format("\"value\":\"%s\",", valueBase64)+
                String.format("\"headers\":%s", jsonHeadersStr())+
                "}").getBytes(JSON_ENCODING);
    }

    private byte[] jsonWithEmptyHeaders() {
        return ("{"+
                String.format("\"topic\":\"%s\",", topic)+
                String.format("\"partition\":%d,", partition)+
                String.format("\"offset\":%d,", offset)+
                String.format("\"timestamp_type\":\"%s\",", timestampType)+
                String.format("\"timestamp\":%d,", timestamp)+
                String.format("\"key\":\"%s\",", keyBase64)+
                String.format("\"value\":\"%s\",", valueBase64)+
                String.format("\"headers\":%s", "[]")+
                "}").getBytes(JSON_ENCODING);
    }

    private byte[] jsonWithNoHeaders() {
        return ("{"+
                String.format("\"topic\":\"%s\",", topic)+
                String.format("\"partition\":%d,", partition)+
                String.format("\"offset\":%d,", offset)+
                String.format("\"timestamp_type\":\"%s\",", timestampType)+
                String.format("\"timestamp\":%d,", timestamp)+
                String.format("\"key\":\"%s\",", keyBase64)+
                String.format("\"value\":\"%s\"", valueBase64)+
                "}").getBytes(JSON_ENCODING);
    }

    @Test
    public void readTest() throws Exception {
        // GIVEN
        InputStream inputStream = new ByteArrayInputStream(jsonWithAllFields());

        // WHEN
        Record actual = sutSerde.read(inputStream);

        // THEN
        Record expected = new Record(topic, partition, keyBytes, valueBytes, offset, timestamp, timestampType, headers);
        assertEquals(expected, actual);
    }

    @Test
    public void writeTest() throws Exception {
        // GIVEN
        Record record = new Record(topic, partition, keyBytes, valueBytes, offset, timestamp, timestampType, headers);

        // WHEN
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        sutSerde.write(outputStream, record);
        byte[] actual = outputStream.toByteArray();

        // THEN
        // NOTE: here we make some (semi-dangerous) assumptions regarding
        // - deterministic key ordering, and
        // - compact formatting without white-space
        byte[] expected = jsonWithAllFields();
        assertArrayEquals(expected, actual);
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

