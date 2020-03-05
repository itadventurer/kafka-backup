package de.azapps.kafkabackup.common.record;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Header;

import java.io.IOException;

public class ConnectHeadersJSONSerializer extends StdSerializer<ConnectHeaders> {
    public ConnectHeadersJSONSerializer() { super(ConnectHeaders.class);  }

    @Override
    public void serialize(ConnectHeaders headers, JsonGenerator jgen, SerializerProvider provider) throws IOException {
        // NOTE: we are serializing headers using this schema: [{key: String, value: String}, ...]
        // instead of the more compact, and json-like, format {[key]: value, ...}
        // Partly because its easier to parse using a static schema, and
        // partly because we cannot(?) guarantee that the ConnectHeader keys are valid json fields.
        jgen.writeStartArray();
        for (Header header : headers) {
            jgen.writeStartObject();
            jgen.writeStringField(ConnectHeadersJSONSerde.HEADER_KEY_PROPERTY, header.key());
            if (header.schema() == Schema.BYTES_SCHEMA || header.schema() == Schema.OPTIONAL_BYTES_SCHEMA) {
                if (header.value() != null) {
                    jgen.writeBinaryField(ConnectHeadersJSONSerde.HEADER_VALUE_PROPERTY, (byte[]) header.value());
                } else {
                    // Lets be kind and handle this, regardless if schema is OPTIONAL_BYTES_SCHEMA or not
                    jgen.writeNullField(ConnectHeadersJSONSerde.HEADER_VALUE_PROPERTY);
                }
            } else {
                throw new IOException(String.format("Unsupported header schema %s", header.schema()));
            }
            jgen.writeEndObject();
        }
        jgen.writeEndArray();
    }
}
