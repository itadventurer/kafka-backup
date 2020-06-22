package de.azapps.kafkabackup.common.record;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;

import java.io.*;
import java.nio.charset.StandardCharsets;

/**
 * Record Format:
 * offset: int64
 * timestampType: int32 -2 if timestamp is null
 * [timestamp: int64] if timestampType != NO_TIMESTAMP_TYPE && timestamp != null
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
        Long timestamp;
        // See comment in `write()`
        if (timestampTypeInt == -2) {
            timestampType = TimestampType.CREATE_TIME;
            timestamp=null;
        } else {
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
            if (timestampType != TimestampType.NO_TIMESTAMP_TYPE) {
                timestamp = dataStream.readLong();
            } else {
                timestamp = null;
            }
        }
        int keyLength = dataStream.readInt();
        byte[] key = null;
        if (keyLength >= 0) {
            key = new byte[keyLength];
            int readBytes = dataStream.read(key);
            if (readBytes != keyLength) {
                throw new IOException(String.format("Expected to read %d bytes, got %d", keyLength, readBytes));
            }
        }

        int valueLength = dataStream.readInt();
        byte[] value = null;
        if (valueLength >= 0) {
            value = new byte[valueLength];
            int readBytes = dataStream.read(value);
            if (readBytes != valueLength) {
                throw new IOException(String.format("Expected to read %d bytes, got %d", valueLength, readBytes));
            }
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
            int readBytes = dataStream.read(headerKeyBytes);
            if (readBytes != headerKeyLength) {
                throw new IOException(String.format("Expected to read %d bytes, got %d", headerKeyLength, readBytes));
            }
            String headerKey = new String(headerKeyBytes, StandardCharsets.UTF_8);
            // Value
            int headerValueLength = dataStream.readInt();
            byte[] headerValue = null;
            if (headerValueLength >= 0) {
                headerValue = new byte[headerValueLength];
                int hvReadBytes = dataStream.read(headerValue);
                if (hvReadBytes != headerValueLength) {
                    throw new IOException(String.format("Expected to read %d bytes, got %d", headerValueLength, hvReadBytes));
                }
            }
            headers.add(headerKey, headerValue);
        }

        return new Record(topic, partition, key, value, offset, timestamp, timestampType, headers);
    }

    public static void write(OutputStream outputStream, Record record) throws IOException {
        DataOutputStream dataStream = new DataOutputStream(outputStream);
        dataStream.writeLong(record.kafkaOffset());
        // There is a special case where the timestamp type eqauls `CREATE_TIME` but is actually `null`.
        // This should not happen normally and I see it as a bug in the Client implementation of pykafka
        // But as Kafka accepts that value, so should Kafka Backup. Thus, this dirty workaround: we write the
        // timestamp type `-2` if the type is CREATE_TIME but the timestamp itself is null. Otherwise we would have
        // needed to change the byte format and for now I think this is the better solution.
        if (record.timestampType() == TimestampType.CREATE_TIME && record.timestamp() == null) {
            dataStream.writeInt(-2);
        } else {
            dataStream.writeInt(record.timestampType().id);
            if (record.timestampType() != TimestampType.NO_TIMESTAMP_TYPE) {
                dataStream.writeLong(record.timestamp());
            }
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
