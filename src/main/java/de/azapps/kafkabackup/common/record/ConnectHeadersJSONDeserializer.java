package de.azapps.kafkabackup.common.record;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import org.apache.kafka.connect.header.ConnectHeaders;

import java.io.IOException;

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
            headers.add(jp.readValueAs(ConnectHeader.class));
            token = jp.nextToken();
        }
        return headers;
    }
}