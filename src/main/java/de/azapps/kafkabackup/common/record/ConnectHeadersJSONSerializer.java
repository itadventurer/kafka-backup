package de.azapps.kafkabackup.common.record;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
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
            jgen.writeObject(header);
        }
        jgen.writeEndArray();
    }
}
