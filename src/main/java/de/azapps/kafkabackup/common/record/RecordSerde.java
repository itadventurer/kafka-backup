package de.azapps.kafkabackup.common.record;

import de.azapps.kafkabackup.common.AlreadyBytesConverter;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.storage.Converter;

import java.io.*;
import java.nio.charset.StandardCharsets;

/**
 * Record Format:
 * offset: int64
 * timestamp: int64
 * timestampType: int32
 * keyLength: int32
 * key: byte[keyLength]
 * valueLength: int32
 * value: byte[valueLength]
 * headerCount: int32
 * headers: Header[headerCount]
 * <p>
 * Header Format:
 * headerKeyLength: int32
 * headerKey: byte[headerKeyLength]
 * headerValueLength: int32
 * headerValue: byte[headerValueLength]
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
        byte[] key;
        if (keyLength == 0) {
            key = null;
        } else {
            key = new byte[keyLength];
            dataStream.read(key);
        }

        int valueLength = dataStream.readInt();
        byte[] value;
        if (valueLength == -1) {
            value = null;
        } else if (valueLength == 0) {
            value = new byte[0];
        } else {
            value = new byte[valueLength];
            dataStream.read(value);
        }
        int headerCount = dataStream.readInt();
        Headers headers = new ConnectHeaders();
        for (int i = 0; i < headerCount; i++) {
            int headerKeyLength = dataStream.readInt();
            byte[] headerKeyBytes = new byte[headerKeyLength];
            dataStream.read(headerKeyBytes);
            String headerKey = new String(headerKeyBytes, StandardCharsets.UTF_8);
            int headerValueLength = dataStream.readInt();
            byte[] headerValue = new byte[headerValueLength];
            dataStream.read(headerValue);
            Header header = new ConnectHeader(headerKey, new SchemaAndValue(Schema.BYTES_SCHEMA, headerValue));
            headers.add(header);
        }

        return new Record(topic, partition, key, value, offset, timestamp, timestampType, headers);
    }

    public static void write(OutputStream outputStream, Record record) throws IOException {
        Converter converter = new AlreadyBytesConverter();
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
            dataStream.writeInt(0);
        }
        if (record.value() == null) {
            dataStream.writeInt(-1);
        } else if (record.value().length == 0) {
            dataStream.writeInt(0);
        } else {
            dataStream.writeInt(record.value().length);
            dataStream.write(record.value());
        }
        dataStream.writeInt(record.headers().size());
        for (Header header : record.headers()) {
            byte[] headerKeyBytes = header.key().getBytes(StandardCharsets.UTF_8);
            dataStream.writeInt(headerKeyBytes.length);
            dataStream.write(headerKeyBytes);
            byte[] headerValueBytes = converter.fromConnectData(record.topic(), header.schema(), header.value());
            dataStream.writeInt(headerValueBytes.length);
            dataStream.write(headerValueBytes);
        }
    }
}
