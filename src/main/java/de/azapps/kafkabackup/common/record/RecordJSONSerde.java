package de.azapps.kafkabackup.common.record;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import de.azapps.kafkabackup.common.AlreadyBytesConverter;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.storage.Converter;

import java.util.Map;
import java.io.*;
import java.nio.charset.StandardCharsets;

public class RecordJSONSerde {
    public static Record read(InputStream inputStream) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        module.addDeserializer(Record.class, new RecordJSONDeserializer());
        mapper.registerModule(module);
        Record record = mapper.readValue(inputStream, Record.class);
        return record;
    }

    public static void write(OutputStream outputStream, Record record) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        module.addSerializer(Record.class, new RecordJSONSerializer());
        mapper.registerModule(module);
        mapper.writeValue(outputStream, record);
    }
}
