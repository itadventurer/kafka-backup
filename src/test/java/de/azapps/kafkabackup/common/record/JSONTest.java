package de.azapps.kafkabackup.common.record;

import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.header.ConnectHeaders;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;

public class JSONTest {
    protected static final String topic = "test-topic";
    protected static final int partition = 42;
    protected static final long offset = 123;
    protected static final TimestampType timestampType = TimestampType.LOG_APPEND_TIME;
    protected static final Long timestamp = 573831430000L;
    // encoding here is not really important, we just want some bytes
    protected static final byte[] keyBytes = "test-key".getBytes(StandardCharsets.UTF_8);
    protected static final byte[] valueBytes = "test-value".getBytes(StandardCharsets.UTF_8);
    // using Base64 as encoding in the json is part of our Record serialization format however:
    protected static final String keyBase64 = Base64.getEncoder().encodeToString(keyBytes);
    protected static final String valueBase64 = Base64.getEncoder().encodeToString(valueBytes);
    // Header fixtures:
    protected static final String header0Key = "header0-key";
    protected static final String header1Key = "header1-key";
    protected static final String header2Key = "header2-key";
    protected static final byte[] header0ValueBytes = "header0-value".getBytes(StandardCharsets.UTF_8);
    protected static final byte[] header1ValueBytes = "header1-value".getBytes(StandardCharsets.UTF_8);
    protected static final byte[] header2ValueBytes = null;
    protected static final String header0ValueBase64 = Base64.getEncoder().encodeToString(header0ValueBytes);
    protected static final String header1ValueBase64 = Base64.getEncoder().encodeToString(header1ValueBytes);
    // header2Value has no base64 encoding
    protected static final ConnectHeaders headers = new ConnectHeaders();
    static {
        headers.add(header0Key, new SchemaAndValue(Schema.OPTIONAL_BYTES_SCHEMA, header0ValueBytes));
        headers.add(header1Key, new SchemaAndValue(Schema.OPTIONAL_BYTES_SCHEMA, header1ValueBytes));
        headers.add(header2Key, new SchemaAndValue(Schema.OPTIONAL_BYTES_SCHEMA, header2ValueBytes));
    }

    protected static final Charset JSON_ENCODING = StandardCharsets.UTF_8;

    protected String jsonHeadersStr() {
        return "["+
                String.format("{\"key\":\"%s\",\"value\":\"%s\"},",header0Key, header0ValueBase64)+
                String.format("{\"key\":\"%s\",\"value\":\"%s\"},",header1Key, header1ValueBase64)+
                String.format("{\"key\":\"%s\",\"value\":%s}",header2Key, "null")+
                "]";
    }

    protected byte[] jsonWithAllFields() {
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

    protected byte[] jsonWithNullKeyAndValue() {
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

    protected byte[] jsonWithNoTimestampType() {
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

    protected byte[] jsonWithEmptyHeaders() {
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

    protected byte[] jsonWithNoHeaders() {
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
}
