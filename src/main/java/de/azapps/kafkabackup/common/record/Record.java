package de.azapps.kafkabackup.common.record;

import de.azapps.kafkabackup.common.AlreadyBytesConverter;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.storage.Converter;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Objects;

public class Record {
	private String topic;
	private Integer kafkaPartition;
	private byte[] key;
	private byte[] value;
	private Long timestamp;
	private Headers headers;
	private long kafkaOffset;
	private TimestampType timestampType;

	public Record(String topic, int partition, byte[] key, byte[] value, long kafkaOffset) {
		this(topic, partition, key, value, kafkaOffset, null, TimestampType.NO_TIMESTAMP_TYPE);
	}

	public Record(String topic, int partition, byte[] key, byte[] value, long kafkaOffset, Long timestamp, TimestampType timestampType) {
		this(topic, partition, key, value, kafkaOffset, timestamp, timestampType, null);
	}

	public Record(String topic, int partition, byte[] key, byte[] value, long kafkaOffset, Long timestamp, TimestampType timestampType, Iterable<Header> headers) {
		this.topic = topic;
		this.kafkaPartition = partition;
		this.key = key;
		this.value = value;
		this.timestamp = timestamp;
		if (headers instanceof ConnectHeaders) {
			this.headers = (ConnectHeaders) headers;
		} else {
			this.headers = new ConnectHeaders(headers);
		}
		this.kafkaOffset = kafkaOffset;
		this.timestampType = timestampType;
	}

	public static Record fromSinkRecord(SinkRecord sinkRecord) {
		Converter converter = new AlreadyBytesConverter();
		byte[] key = converter.fromConnectData(sinkRecord.topic(), sinkRecord.keySchema(), sinkRecord.key());
		byte[] value = converter.fromConnectData(sinkRecord.topic(), sinkRecord.valueSchema(), sinkRecord.value());
		return new Record(sinkRecord.topic(), sinkRecord.kafkaPartition(), key, value, sinkRecord.kafkaOffset(), sinkRecord.timestamp(), sinkRecord.timestampType(), sinkRecord.headers());
	}

	public String topic() {
		return topic;
	}

	public Integer kafkaPartition() {
		return kafkaPartition;
	}

	public byte[] key() {
		return key;
	}

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
		if (!Objects.equals(a.schema(), b.schema())) {
			return false;
		}
		try {
			// This particular case is not handled by ConnectHeader::equals
			byte[] aBytes = (byte[]) a.value();
			byte[] bBytes = (byte[]) b.value();
			return Arrays.equals(aBytes, bBytes);
		} catch (ClassCastException e) {
			return Objects.equals(a.value(), b.value());
		}
	}

	@Override
	public String toString() {
		String keyLength = (key == null) ? "null" : String.valueOf(key.length);
		String valueLength = (value == null) ? "null" : String.valueOf(value.length);
		String timestampTypeStr = timestampType.toString();
		String timestampStr = (timestamp == null) ? "null" : String.valueOf(timestamp);
		return String.format("Record{topic: %s, partition: %d, offset: %d, key: byte[%s], value: byte[%s], timestampType: %s, timestamp: %s, headers: %s}", topic, kafkaPartition, kafkaOffset, keyLength, valueLength, timestampTypeStr, timestampStr, headers);
	}
}
