package de.azapps.kafkabackup.common.record;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.node.IntNode;

import java.io.IOException;
import java.util.Base64;

public class RecordJSONDeserializer extends StdDeserializer<Record> {

    public RecordJSONDeserializer() {
        this(null);
    }

    public RecordJSONDeserializer(Class<?> vc) {
        super(vc);
    }

    @Override
    public Record deserialize(JsonParser jp, DeserializationContext ctx) throws IOException, JsonProcessingException {
        JsonNode node = jp.getCodec().readTree(jp);
        String topic = node.get("topic").asText();
        int partition = node.get("partition").asInt();
        long offset = node.get("offset").asLong();
        String keyBase64 = node.get("key").asText();
        String valueBase64 = node.get("value").asText();

        // TODO: initialize in constructor instead?
        Base64.Decoder decoder = Base64.getDecoder();
        byte[] key = decoder.decode(keyBase64);
        byte[] value = decoder.decode(valueBase64);

        // TODO: parse timestamp, timstampType and headers as well
        return new Record(topic, partition, key, value, offset);
    }
}