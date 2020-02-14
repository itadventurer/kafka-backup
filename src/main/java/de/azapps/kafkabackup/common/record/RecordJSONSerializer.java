package de.azapps.kafkabackup.common.record;


import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;

public class RecordJSONSerializer extends StdSerializer<Record> {

    public RecordJSONSerializer() {
        this(null);
    }

    public RecordJSONSerializer(Class<Record> t) {
        super(t);
    }

    @Override
    public void serialize(Record record, JsonGenerator jgen, SerializerProvider provider) throws IOException, JsonProcessingException {
        jgen.writeStartObject();
        jgen.writeStringField("topic", record.topic());
        jgen.writeNumberField("partition", record.kafkaPartition());
        jgen.writeNumberField("offset", record.kafkaOffset());
        // key and value should be base64-encoded, and jackson provides this convenience helper.
        // see: https://javadoc.io/doc/com.fasterxml.jackson.core/jackson-core/2.10.1/index.html
        jgen.writeBinaryField("key", record.key());
        jgen.writeBinaryField("value", record.value());
        // TODO: add timestamp, timestampType and headers
        jgen.writeEndObject();
    }
}