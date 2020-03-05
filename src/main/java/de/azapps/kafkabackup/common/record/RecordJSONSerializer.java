package de.azapps.kafkabackup.common.record;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;

public class RecordJSONSerializer extends StdSerializer<Record> {
    public RecordJSONSerializer() {
        super(Record.class);
    }

    @Override
    public void serialize(Record record, JsonGenerator jgen, SerializerProvider provider) throws IOException, JsonProcessingException {
        jgen.writeStartObject();
        jgen.writeStringField(RecordJSONSerde.TOPIC_PROPERTY, record.topic());
        jgen.writeNumberField(RecordJSONSerde.PARTITION_PROPERTY, record.kafkaPartition());
        jgen.writeNumberField(RecordJSONSerde.OFFSET_PROPERTY, record.kafkaOffset());
        jgen.writeStringField(RecordJSONSerde.TIMESTAMP_TYPE_PROPERTY, record.timestampType().toString());
        if (record.timestamp() != null) {
            jgen.writeNumberField(RecordJSONSerde.TIMESTAMP_PROPERTY, record.timestamp());
        } else {
            jgen.writeNullField(RecordJSONSerde.TIMESTAMP_PROPERTY);
        }
        // key and value should be base64-encoded, and jackson provides the `writeBinaryField` convenience helper.
        // see: https://javadoc.io/doc/com.fasterxml.jackson.core/jackson-core/2.10.1/index.html
        // Furthermore, its ok if they are null.
        if (record.key() == null) {
            jgen.writeNullField(RecordJSONSerde.KEY_PROPERTY);
        } else {
            jgen.writeBinaryField(RecordJSONSerde.KEY_PROPERTY, record.key());
        }
        if (record.value() == null) {
            jgen.writeNullField(RecordJSONSerde.VALUE_PROPERTY);
        } else {
            jgen.writeBinaryField(RecordJSONSerde.VALUE_PROPERTY, record.value());
        }
        provider.defaultSerializeField(RecordJSONSerde.HEADERS_PROPERTY, record.headers(), jgen);
    }
}