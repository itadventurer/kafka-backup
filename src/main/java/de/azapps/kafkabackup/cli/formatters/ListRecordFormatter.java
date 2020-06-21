package de.azapps.kafkabackup.cli.formatters;

import de.azapps.kafkabackup.common.record.Record;

import java.io.PrintStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

public class ListRecordFormatter extends RecordFormatter {
    private final DateFormat timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public ListRecordFormatter(ByteFormatter keyFormatter, ByteFormatter valueFormatter) {
        super(keyFormatter, valueFormatter);
    }

    @Override
    public void writeTo(Record record, PrintStream outputStream) {
        String offset = "Offset: " + record.kafkaOffset();
        String key;
        if (record.key() == null) {
            key = "NULL Key";
        } else {
            key = "Key: " + keyFormatter.toString(record.key());
        }
        String timestamp = "Timestamp: ";
        System.out.println(record);

        switch (record.timestampType()) {
            case NO_TIMESTAMP_TYPE:
                timestamp += "No Timestamp";
                break;
            case CREATE_TIME:
                timestamp += "(create)";
                timestamp += timestampFormat.format(record.timestamp());
                break;
            case LOG_APPEND_TIME:
                timestamp += "(log append)";
                timestamp += timestampFormat.format(record.timestamp());
                break;
        }
        String data_length;
        if (record.value() == null) {
            data_length = "NULL Value";
        } else {
            data_length = "Data: " + valueFormatter.toString(record.value());
        }

        outputStream.println(offset + " " + key + " " + timestamp + " " + data_length);
    }
}
