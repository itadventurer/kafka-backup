package de.azapps.kafkabackup.common.record;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.kafka.connect.header.ConnectHeaders;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class RecordJSONSerde {
    private ObjectMapper mapper;
    protected static final String TOPIC_PROPERTY = "topic";
    protected static final String PARTITION_PROPERTY = "partition";
    protected static final String OFFSET_PROPERTY = "offset";
    protected static final String TIMESTAMP_TYPE_PROPERTY = "timestamp_type";
    protected static final String TIMESTAMP_PROPERTY = "timestamp";
    protected static final String KEY_PROPERTY = "key";
    protected static final String VALUE_PROPERTY = "value";
    protected static final String HEADERS_PROPERTY = "headers";

    public RecordJSONSerde() {
        this.mapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        module.addDeserializer(Record.class, new RecordJSONDeserializer());
        module.addDeserializer(ConnectHeaders.class, new ConnectHeadersJSONDeserializer());
        module.addSerializer(Record.class, new RecordJSONSerializer());
        module.addSerializer(ConnectHeaders.class, new ConnectHeadersJSONSerializer());
        mapper.registerModule(module);
    }

    public Record read(InputStream inputStream) throws IOException {
        return mapper.readValue(inputStream, Record.class);
    }

    public void write(OutputStream outputStream, Record record) throws IOException {
        mapper.writeValue(outputStream, record);
    }
}
