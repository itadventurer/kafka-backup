package de.azapps.kafkabackup.common.record;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;

import java.io.IOException;
import java.util.Base64;

public class ConnectHeaderJSONDeserializer extends StdDeserializer<ConnectHeader> {
    public ConnectHeaderJSONDeserializer() {
        super(ConnectHeader.class);
    }

    @Override
    public ConnectHeader deserialize(JsonParser jp, DeserializationContext ctx) throws IOException {
        JsonNode node = jp.getCodec().readTree(jp);
        String key = node.get(ConnectHeaderJSONSerde.HEADER_KEY_PROPERTY).asText(null);
        String valueBase64 = node.get(ConnectHeaderJSONSerde.HEADER_VALUE_PROPERTY).asText(null);
        byte[] value = (valueBase64 == null) ? null : Base64.getDecoder().decode(valueBase64);

        return new ConnectHeader(key, new SchemaAndValue(Schema.OPTIONAL_BYTES_SCHEMA, value));
    }
}