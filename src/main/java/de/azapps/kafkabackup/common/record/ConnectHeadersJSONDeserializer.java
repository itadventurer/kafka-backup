package de.azapps.kafkabackup.common.record;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.header.ConnectHeaders;

import java.io.IOException;
import java.util.Base64;

public class ConnectHeadersJSONDeserializer extends StdDeserializer<ConnectHeaders> {
    public ConnectHeadersJSONDeserializer() {
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
            JsonNode node = jp.getCodec().readTree(jp);
            String key = node.get(ConnectHeadersJSONSerde.HEADER_KEY_PROPERTY).asText(null);
            String valueBase64 = node.get(ConnectHeadersJSONSerde.HEADER_VALUE_PROPERTY).asText(null);
            byte[] value = (valueBase64 == null) ? null : Base64.getDecoder().decode(valueBase64);
            headers.add(key, new SchemaAndValue(Schema.OPTIONAL_BYTES_SCHEMA, value));

            token = jp.nextToken();
        }
        return headers;
    }
}