package de.azapps.kafkabackup.common.record;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Objects;

public class Record {
    private final String topic;
    private final Integer kafkaPartition;
    private final byte[] key;
    private final byte[] value;
    private final Long timestamp;
    private final Headers headers;
    private final long kafkaOffset;
    private final TimestampType timestampType;

    public Record(String topic, int partition, byte[] key, byte[] value, long kafkaOffset) {
        this(topic, partition, key, value, kafkaOffset, null, TimestampType.NO_TIMESTAMP_TYPE);
    }

    public Record(String topic, int partition, byte[] key, byte[] value, long kafkaOffset, Long timestamp, TimestampType timestampType) {
        this(topic, partition, key, value, kafkaOffset, timestamp, timestampType, new RecordHeaders());
    }

    // We do not want to copy the data and assume that Kafka Connect is not malicious
    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings("EI_EXPOSE_REP")
    public Record(String topic, int partition, byte[] key, byte[] value, long kafkaOffset, Long timestamp, TimestampType timestampType, Headers headers) {
        this.topic = topic;
        this.kafkaPartition = partition;
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
        this.headers = headers;
        this.kafkaOffset = kafkaOffset;
        this.timestampType = timestampType;
    }

    public static Record fromSinkRecord(SinkRecord sinkRecord) {
        byte[] key = connectDataToBytes(sinkRecord.keySchema(), sinkRecord.key());
        byte[] value = connectDataToBytes(sinkRecord.valueSchema(), sinkRecord.value());
        RecordHeaders recordHeaders = new RecordHeaders();
        for (org.apache.kafka.connect.header.Header connectHeader : sinkRecord.headers()) {
            byte[] headerValue = connectDataToBytes(connectHeader.schema(), connectHeader.value());
            recordHeaders.add(connectHeader.key(), headerValue);
        }
        return new Record(sinkRecord.topic(), sinkRecord.kafkaPartition(), key, value, sinkRecord.kafkaOffset(), sinkRecord.timestamp(), sinkRecord.timestampType(), recordHeaders);
    }

    private static byte[] connectDataToBytes(Schema schema, Object value) {
        if (schema != null && schema.type() != Schema.Type.BYTES)
            throw new DataException("Invalid schema type for ByteArrayConverter: " + schema.type().toString());

        if (value != null && !(value instanceof byte[]))
            throw new DataException("ByteArrayConverter is not compatible with objects of type " + value.getClass());

        return (byte[]) value;
    }

    public SinkRecord toSinkRecord() {
        ConnectHeaders connectHeaders = new ConnectHeaders();
        for (Header header : headers) {
            connectHeaders.addBytes(header.key(), header.value());
        }
        return new SinkRecord(topic, kafkaPartition, Schema.OPTIONAL_BYTES_SCHEMA, key, Schema.OPTIONAL_BYTES_SCHEMA, value, kafkaOffset,
                timestamp, timestampType, connectHeaders);
    }

    public String topic() {
        return topic;
    }

    public Integer kafkaPartition() {
        return kafkaPartition;
    }

    // We do not want to copy the data and assume that Kafka Connect is not malicious
    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings("EI_EXPOSE_REP")
    public byte[] key() {
        return key;
    }

    // We do not want to copy the data and assume that Kafka Connect is not malicious
    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings("EI_EXPOSE_REP")
    public byte[] value() {
        return value;
    }

    public Long timestamp() {
        return timestamp;
    }

    public Headers headers() {
        return headers;
    }

    public long kafkaOffset() {
        return kafkaOffset;
    }

    public TimestampType timestampType() {
        return timestampType;
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(topic, kafkaPartition, timestamp, headers, kafkaOffset, timestampType);
        result = 31 * result + Arrays.hashCode(key);
        result = 31 * result + Arrays.hashCode(value);
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        Record that = (Record) o;

        // alternative implementation of ConnectRecord.equals that use Headers equality by value
        return Objects.equals(kafkaPartition(), that.kafkaPartition())
                && Objects.equals(topic(), that.topic())
                && Arrays.equals(key(), that.key())
                && Arrays.equals(value(), that.value())
                && Objects.equals(timestamp(), that.timestamp())
                && headersEqualityByValue(headers(), that.headers())
                && Objects.equals(kafkaOffset(), that.kafkaOffset())
                && Objects.equals(timestampType(), that.timestampType());
    }

    @Override
    public String toString() {
        String keyLength = (key == null) ? "null" : String.valueOf(key.length);
        String valueLength = (value == null) ? "null" : String.valueOf(value.length);
        String timestampTypeStr = timestampType.toString();
        String timestampStr = (timestamp == null) ? "null" : String.valueOf(timestamp);
        return String.format("Record{topic: %s, partition: %d, offset: %d, key: byte[%s], value: byte[%s], timestampType: %s, timestamp: %s, headers: %s}",
                topic, kafkaPartition, kafkaOffset, keyLength, valueLength, timestampTypeStr, timestampStr, headers);
    }

    private boolean headersEqualityByValue(Headers a, Headers b) {
        // This is an alternative implementation of ConnectHeaders::equals that use proper Header equality by value
        if (a == b) {
            return true;
        }
        // Note, similar to ConnectHeaders::equals, it requires headers to have the same order
        // (although, that is probably not what we want in most cases)
        Iterator<Header> aIter = a.iterator();
        Iterator<Header> bIter = b.iterator();
        while (aIter.hasNext() && bIter.hasNext()) {
            if (!headerEqualityByValue(aIter.next(), bIter.next()))
                return false;
        }
        return !aIter.hasNext() && !bIter.hasNext();
    }

    private boolean headerEqualityByValue(Header a, Header b) {
        // This is an alternative implementation of ConnectHeader::equals that use proper Value equality by value
        // (even if they are byte arrays)
        if (a == b) {
            return true;
        }
        if (!Objects.equals(a.key(), b.key())) {
            return false;
        }
        try {
            // This particular case is not handled by ConnectHeader::equals
            byte[] aBytes = a.value();
            byte[] bBytes = b.value();
            return Arrays.equals(aBytes, bBytes);
        } catch (ClassCastException e) {
            return a.value() == b.value();
        }
    }


}
