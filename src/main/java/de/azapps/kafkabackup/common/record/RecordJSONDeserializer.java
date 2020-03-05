package de.azapps.kafkabackup.common.record;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.header.ConnectHeaders;

import java.io.IOException;
import java.util.Base64;
import java.util.Optional;

public class RecordJSONDeserializer extends StdDeserializer<Record> {
    public RecordJSONDeserializer() {
        super(Record.class);
    }

    @Override
    public Record deserialize(JsonParser jp, DeserializationContext ctx) throws IOException {
        JsonNode node = jp.getCodec().readTree(jp);
        String topic = node.get(RecordJSONSerde.TOPIC_PROPERTY).asText();
        int partition = node.get(RecordJSONSerde.PARTITION_PROPERTY).asInt();
        long offset = node.get(RecordJSONSerde.OFFSET_PROPERTY).asLong();

        // Default seems to be the string "null", which is not wanted here
        String timestampTypeNullableString = node.get(RecordJSONSerde.TIMESTAMP_TYPE_PROPERTY).asText(null);
        TimestampType timestampType = Optional.ofNullable(timestampTypeNullableString)
                .map(TimestampType::forName)
                .orElse(TimestampType.NO_TIMESTAMP_TYPE);
        // `Long` used instead of primitive `long` to use it as a kind of "optional" with null as a legal value
        Long timestamp = (node.hasNonNull(RecordJSONSerde.TIMESTAMP_PROPERTY)) ? (Long) node.get(RecordJSONSerde.TIMESTAMP_PROPERTY).asLong() : null;

        // TODO: is getting a decoder expensive?
        String keyBase64 = node.get(RecordJSONSerde.KEY_PROPERTY).asText(null); // Default seems to be the string "null", which is not wanted here
        byte[] key = (keyBase64 == null) ? null : Base64.getDecoder().decode(keyBase64);
        String valueBase64 = node.get(RecordJSONSerde.VALUE_PROPERTY).asText(null);
        byte[] value = (valueBase64 == null) ? null : Base64.getDecoder().decode(valueBase64);

        // Parse headers using registered deserializer
        ConnectHeaders headers = Optional.ofNullable(node.findValue(RecordJSONSerde.HEADERS_PROPERTY))
                .map(n -> {
                    try {
                        return n.traverse(jp.getCodec()).readValueAs(ConnectHeaders.class);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                })
                .orElseGet(ConnectHeaders::new);

        return new Record(topic, partition, key, value, offset, timestamp, timestampType, headers);
    }
}