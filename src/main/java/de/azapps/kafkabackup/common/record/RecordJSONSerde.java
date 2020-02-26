package de.azapps.kafkabackup.common.record;


import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import org.apache.kafka.common.record.TimestampType;

import java.util.Base64;
import java.io.*;

public class RecordJSONSerde {
    private ObjectMapper mapper;
    private static final String TOPIC_PROPERTY = "topic";
    private static final String PARTITION_PROPERTY = "partition";
    private static final String OFFSET_PROPERTY = "offset";
    private static final String TIMESTAMP_TYPE_PROPERTY = "timestamp_type";
    private static final String TIMESTAMP_PROPERTY = "timestamp";
    private static final String KEY_PROPERTY = "key";
    private static final String VALUE_PROPERTY = "value";

    public RecordJSONSerde() {
        this.mapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        module.addDeserializer(Record.class, new Deserializer());
        module.addSerializer(Record.class, new Serializer());
        mapper.registerModule(module);
    }

    public Record read(InputStream inputStream) throws IOException {
        return mapper.readValue(inputStream, Record.class);
    }

    public void write(OutputStream outputStream, Record record) throws IOException {
        mapper.writeValue(outputStream, record);
    }

    public String writeValueAsString(Record record) throws JsonProcessingException {
        return mapper.writeValueAsString(record);
    }

    public static class Deserializer extends StdDeserializer<Record> {
        public Deserializer() {
            super(Record.class);
        }

        @Override
        public Record deserialize(JsonParser jp, DeserializationContext ctx) throws IOException, JsonProcessingException {
            JsonNode node = jp.getCodec().readTree(jp);
            String topic = node.get(TOPIC_PROPERTY).asText();
            int partition = node.get(PARTITION_PROPERTY).asInt();
            long offset = node.get(OFFSET_PROPERTY).asLong();
            String timestampTypeStr = node.get(TIMESTAMP_TYPE_PROPERTY).asText(null); // Default seems to be the string "null", which is not wanted here
            TimestampType timestampType;
            if (timestampTypeStr != null) {
                timestampType = TimestampType.forName(timestampTypeStr);
            } else {
                timestampType = TimestampType.NO_TIMESTAMP_TYPE;
            }
            Long timestamp; // used instead of primitive long to use it as an Optional with null as a legal value
            if (node.hasNonNull(TIMESTAMP_PROPERTY)) {
                timestamp = node.get(TIMESTAMP_PROPERTY).asLong();
            } else {
                timestamp = null;
            }
            String keyBase64 = node.get(KEY_PROPERTY).asText(null); // Default seems to be the string "null", which is not wanted here
            String valueBase64 = node.get(VALUE_PROPERTY).asText(null);
            // TODO: parse timestamp, timstampType and headers as well
            // TODO: is getting a decoder expensive?
            byte[] key = (keyBase64 == null) ? null : Base64.getDecoder().decode(keyBase64);
            byte[] value = (valueBase64 == null) ? null : Base64.getDecoder().decode(valueBase64);

            return new Record(topic, partition, key, value, offset, timestamp, timestampType);
        }
    }

    public static class Serializer extends StdSerializer<Record> {
        public Serializer() {
            super(Record.class);
        }

        @Override
        public void serialize(Record record, JsonGenerator jgen, SerializerProvider provider) throws IOException, JsonProcessingException {
            jgen.writeStartObject();
            jgen.writeStringField(TOPIC_PROPERTY, record.topic());
            jgen.writeNumberField(PARTITION_PROPERTY, record.kafkaPartition());
            jgen.writeNumberField(OFFSET_PROPERTY, record.kafkaOffset());
            jgen.writeStringField(TIMESTAMP_TYPE_PROPERTY, record.timestampType().toString());
            if (record.timestamp() != null) {
                jgen.writeNumberField(TIMESTAMP_PROPERTY, record.timestamp());
            } else {
                jgen.writeNullField(TIMESTAMP_PROPERTY);
            }
            // key and value should be base64-encoded, and jackson provides the `writeBinaryField` convenience helper.
            // see: https://javadoc.io/doc/com.fasterxml.jackson.core/jackson-core/2.10.1/index.html
            // Furthermore, its ok if they are null.
            if (record.key() == null) {
                jgen.writeNullField(KEY_PROPERTY);
            } else {
                jgen.writeBinaryField(KEY_PROPERTY, record.key());
            }
            if (record.value() == null) {
                jgen.writeNullField(VALUE_PROPERTY);
            } else {
                jgen.writeBinaryField(VALUE_PROPERTY, record.value());
            }
            // TODO: add timestamp, timestampType and headers
            jgen.writeEndObject();
        }
    }
}
