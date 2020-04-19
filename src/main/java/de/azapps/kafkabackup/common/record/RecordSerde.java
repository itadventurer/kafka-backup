package de.azapps.kafkabackup.common.record;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;

import java.io.*;
import java.nio.charset.StandardCharsets;

/**
 * Record Format:
 * offset: int64
 * timestampType: int32
 * [timestamp: int64] if timestampType != NO_TIMESTAMP_TYPE
 * keyLength: int32
 * [key: byte[keyLength]] if keyLength >= 0
 * valueLength: int32
 * [value: byte[valueLength]] if valueLength >= 0
 * headerCount: int32
 * headers: Header[headerCount]
 * <p>
 * Header Format:
 * headerKeyLength: int32
 * headerKey: byte[headerKeyLength]
 * headerValueLength: int32
 * [headerValue: byte[headerValueLength]] if headerValueLength >= 0
 */
public class RecordSerde {
    public static Record read(String topic, int partition, InputStream inputStream) throws IOException {
        DataInputStream dataStream = new DataInputStream(inputStream);
        long offset = dataStream.readLong();
        int timestampTypeInt = dataStream.readInt();
        TimestampType timestampType;
        switch (timestampTypeInt) {
            case -1:
                timestampType = TimestampType.NO_TIMESTAMP_TYPE;
                break;
            case 0:
                timestampType = TimestampType.CREATE_TIME;
                break;
            case 1:
                timestampType = TimestampType.LOG_APPEND_TIME;
                break;
            default:
                throw new RuntimeException("Unexpected TimestampType. Expected -1,0 or 1. Got " + timestampTypeInt);
        }
        Long timestamp;
        if (timestampType != TimestampType.NO_TIMESTAMP_TYPE) {
            timestamp = dataStream.readLong();
        } else {
            timestamp = null;
        }
        int keyLength = dataStream.readInt();
        byte[] key = null;
        if (keyLength >= 0) {
            key = new byte[keyLength];
            dataStream.read(key);
        }

        int valueLength = dataStream.readInt();
        byte[] value = null;
        if (valueLength >= 0) {
            value = new byte[valueLength];
            dataStream.read(value);
        }
        int headerCount = dataStream.readInt();
        RecordHeaders headers = new RecordHeaders();
        for (int i = 0; i < headerCount; i++) {
            // Key
            int headerKeyLength = dataStream.readInt();
            if (headerKeyLength < 0) {
                throw new RuntimeException("Invalid negative header key size " + headerKeyLength);
            }
            byte[] headerKeyBytes = new byte[headerKeyLength];
            dataStream.read(headerKeyBytes);
            String headerKey = new String(headerKeyBytes, StandardCharsets.UTF_8);
            // Value
            int headerValueLength = dataStream.readInt();
            byte[] headerValue = null;
            if (headerValueLength >= 0) {
                headerValue = new byte[headerValueLength];
                dataStream.read(headerValue);
            }
            headers.add(headerKey, headerValue);
        }

        return new Record(topic, partition, key, value, offset, timestamp, timestampType, headers);
    }

    public static void write(OutputStream outputStream, Record record) throws IOException {
        DataOutputStream dataStream = new DataOutputStream(outputStream);
        dataStream.writeLong(record.kafkaOffset());
        dataStream.writeInt(record.timestampType().id);
        if (record.timestampType() != TimestampType.NO_TIMESTAMP_TYPE) {
            dataStream.writeLong(record.timestamp());
        }
        if (record.key() != null) {
            dataStream.writeInt(record.key().length);
            dataStream.write(record.key());
        } else {
            dataStream.writeInt(-1);
        }
        if (record.value() != null) {
            dataStream.writeInt(record.value().length);
            dataStream.write(record.value());
        } else {
            dataStream.writeInt(-1);
        }
        Header[] headers = record.headers().toArray();
        dataStream.writeInt(headers.length);
        for (Header header : record.headers()) {
            byte[] headerKeyBytes = header.key().getBytes(StandardCharsets.UTF_8);
            dataStream.writeInt(headerKeyBytes.length);
            dataStream.write(headerKeyBytes);
            if (header.value() != null) {
                dataStream.writeInt(header.value().length);
                dataStream.write(header.value());
            } else {
                dataStream.writeInt(-1);
            }
        }
    }
}
