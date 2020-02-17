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

import java.util.Base64;
import java.io.*;

public class RecordJSONSerde {
    private ObjectMapper mapper;
    private static final String TOPIC_PROPERTY = "topic";
    private static final String PARTITION_PROPERTY = "partition";
    private static final String OFFSET_PROPERTY = "offset";
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
            String keyBase64 = node.get(KEY_PROPERTY).asText();
            String valueBase64 = node.get(VALUE_PROPERTY).asText();

            // TODO: initialize in constructor instead?
            Base64.Decoder decoder = Base64.getDecoder();
            byte[] key = decoder.decode(keyBase64);
            byte[] value = decoder.decode(valueBase64);

            // TODO: parse timestamp, timstampType and headers as well
            return new Record(topic, partition, key, value, offset);
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
            // key and value should be base64-encoded, and jackson provides this convenience helper.
            // see: https://javadoc.io/doc/com.fasterxml.jackson.core/jackson-core/2.10.1/index.html
            jgen.writeBinaryField(KEY_PROPERTY, record.key());
            jgen.writeBinaryField(VALUE_PROPERTY, record.value());
            // TODO: add timestamp, timestampType and headers
            jgen.writeEndObject();
        }
    }
}
