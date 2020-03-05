package de.azapps.kafkabackup.common.record;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import org.apache.kafka.connect.data.Schema;

import java.io.IOException;

public class ConnectHeaderJSONSerializer extends StdSerializer<ConnectHeader> {
    public ConnectHeaderJSONSerializer() { super(ConnectHeader.class);  }

    @Override
    public void serialize(ConnectHeader header, JsonGenerator jgen, SerializerProvider provider) throws IOException {
        jgen.writeStartObject();
        jgen.writeStringField(ConnectHeaderJSONSerde.HEADER_KEY_PROPERTY, header.key());
        if (header.schema() == Schema.BYTES_SCHEMA || header.schema() == Schema.OPTIONAL_BYTES_SCHEMA) {
            if (header.value() != null) {
                jgen.writeBinaryField(ConnectHeaderJSONSerde.HEADER_VALUE_PROPERTY, (byte[]) header.value());
            } else {
                // Lets be kind and handle this, regardless if schema is OPTIONAL_BYTES_SCHEMA or not
                jgen.writeNullField(ConnectHeaderJSONSerde.HEADER_VALUE_PROPERTY);
            }
        } else {
            throw new IOException(String.format("Unsupported header schema %s", header.schema()));
        }
        jgen.writeEndObject();
    }
}