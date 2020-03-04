package de.azapps.kafkabackup.common.record;

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Header;

import java.util.Base64;
import java.io.*;
import java.util.Optional;

public class RecordJSONSerde {
    private ObjectMapper mapper;
    private static final String TOPIC_PROPERTY = "topic";
    private static final String PARTITION_PROPERTY = "partition";
    private static final String OFFSET_PROPERTY = "offset";
    private static final String TIMESTAMP_TYPE_PROPERTY = "timestamp_type";
    private static final String TIMESTAMP_PROPERTY = "timestamp";
    private static final String KEY_PROPERTY = "key";
    private static final String VALUE_PROPERTY = "value";
    private static final String HEADERS_PROPERTY = "headers";
    private static final String HEADER_KEY_PROPERTY = "key";
    private static final String HEADER_VALUE_PROPERTY = "value";

    public RecordJSONSerde() {
        this.mapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        module.addDeserializer(Record.class, new RecordDeserializer());
        module.addDeserializer(ConnectHeaders.class, new HeadersDeserializer());
        module.addDeserializer(ConnectHeader.class, new HeaderDeserializer());
        module.addSerializer(Record.class, new RecordSerializer());
        module.addSerializer(ConnectHeaders.class, new HeadersSerializer());
        module.addSerializer(ConnectHeader.class, new HeaderSerializer());
        mapper.registerModule(module);
    }

    public Record read(InputStream inputStream) throws IOException {
        return mapper.readValue(inputStream, Record.class);
    }

    public void write(OutputStream outputStream, Record record) throws IOException {
        mapper.writeValue(outputStream, record);
    }

    public static class RecordDeserializer extends StdDeserializer<Record> {
        public RecordDeserializer() {
            super(Record.class);
        }

        @Override
        public Record deserialize(JsonParser jp, DeserializationContext ctx) throws IOException {
            JsonNode node = jp.getCodec().readTree(jp);
            String topic = node.get(TOPIC_PROPERTY).asText();
            int partition = node.get(PARTITION_PROPERTY).asInt();
            long offset = node.get(OFFSET_PROPERTY).asLong();

            String timestampTypeNullableString = node.get(TIMESTAMP_TYPE_PROPERTY).asText(null); // Default seems to be the string "null", which is not wanted here
            TimestampType timestampType = Optional.ofNullable(timestampTypeNullableString)
                    .map(TimestampType::forName)
                    .orElse(TimestampType.NO_TIMESTAMP_TYPE);
            // `Long` used instead of primitive `long` to use it as a kind of "optional" with null as a legal value
            Long timestamp = (node.hasNonNull(TIMESTAMP_PROPERTY)) ? (Long) node.get(TIMESTAMP_PROPERTY).asLong() : null;

            // TODO: is getting a decoder expensive?
            String keyBase64 = node.get(KEY_PROPERTY).asText(null); // Default seems to be the string "null", which is not wanted here
            byte[] key = (keyBase64 == null) ? null : Base64.getDecoder().decode(keyBase64);
            String valueBase64 = node.get(VALUE_PROPERTY).asText(null);
            byte[] value = (valueBase64 == null) ? null : Base64.getDecoder().decode(valueBase64);

            // Parse headers using registered deserializer
            ConnectHeaders headers = Optional.ofNullable(node.findValue(HEADERS_PROPERTY))
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

    public static class HeadersDeserializer extends StdDeserializer<ConnectHeaders> {
        public HeadersDeserializer() {
            super(ConnectHeaders.class);
        }

        @Override
        public ConnectHeaders deserialize(JsonParser jp, DeserializationContext ctx) throws IOException {
            if (!jp.isExpectedStartArrayToken()) {
                throw new JsonParseException(jp, "Start array expected");
            }
            ConnectHeaders headers = new ConnectHeaders();
            JsonToken token = jp.nextToken();
            while (token != JsonToken.END_ARRAY) {
                headers.add(jp.readValueAs(ConnectHeader.class));
                token = jp.nextToken();
            }
            return headers;
        }
    }

    public static class HeaderDeserializer extends StdDeserializer<ConnectHeader> {
        public HeaderDeserializer() {
            super(Header.class);
        }

        @Override
        public ConnectHeader deserialize(JsonParser jp, DeserializationContext ctx) throws IOException {
            JsonNode node = jp.getCodec().readTree(jp);
            String key = node.get(HEADER_KEY_PROPERTY).asText(null);
            String valueBase64 = node.get(HEADER_VALUE_PROPERTY).asText(null);
            byte[] value = (valueBase64 == null) ? null : Base64.getDecoder().decode(valueBase64);

            return new ConnectHeader(key, new SchemaAndValue(Schema.OPTIONAL_BYTES_SCHEMA, value));
        }
    }

    public static class RecordSerializer extends StdSerializer<Record> {
        public RecordSerializer() {
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
            provider.defaultSerializeField(HEADERS_PROPERTY, record.headers(), jgen);
        }
    }

    // ConnectHeaders => json array
    public static class HeadersSerializer extends StdSerializer<ConnectHeaders> {
        public HeadersSerializer() { super(ConnectHeaders.class);  }

        @Override
        public void serialize(ConnectHeaders headers, JsonGenerator jgen, SerializerProvider provider) throws IOException {
            // NOTE: we are serializing headers using this schema: [{key: String, value: String}, ...]
            // instead of the more compact, and json-like, format {[key]: value, ...}
            // Partly because its easier to parse using a static schema, and
            // partly because we cannot(?) guarantee that the ConnectHeader keys are valid json fields.
            jgen.writeStartArray();
            for (Header header : headers) {
                jgen.writeObject(header);
            }
            jgen.writeEndArray();
        }
    }

    // ConnectHeader => json object
    public static class HeaderSerializer extends StdSerializer<ConnectHeader> {
        public HeaderSerializer() { super(ConnectHeader.class);  }

        @Override
        public void serialize(ConnectHeader header, JsonGenerator jgen, SerializerProvider provider) throws IOException {
            jgen.writeStartObject();
            jgen.writeStringField(HEADER_KEY_PROPERTY, header.key());
            if (header.schema() == Schema.BYTES_SCHEMA || header.schema() == Schema.OPTIONAL_BYTES_SCHEMA) {
                if (header.value() != null) {
                    jgen.writeBinaryField(HEADER_VALUE_PROPERTY, (byte[]) header.value());
                } else {
                    // Lets be kind and handle this, regardless if schema is OPTIONAL_BYTES_SCHEMA or not
                    jgen.writeNullField(HEADER_VALUE_PROPERTY);
                }
            } else {
                throw new IOException(String.format("Unsupported header schema %s", header.schema()));
            }
            jgen.writeEndObject();
        }
    }
}
