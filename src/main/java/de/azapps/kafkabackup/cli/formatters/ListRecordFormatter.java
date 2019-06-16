package de.azapps.kafkabackup.cli.formatters;

import de.azapps.kafkabackup.common.record.Record;

import java.io.PrintStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

public class ListRecordFormatter extends RecordFormatter {
    private DateFormat timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public ListRecordFormatter(ByteFormatter keyFormatter, ByteFormatter valueFormatter) {
        super(keyFormatter, valueFormatter);
    }

    @Override
    public void writeTo(Record record, PrintStream outputStream) {
        outputStream.println("Offset: " + record.kafkaOffset()
                + " Key: " + keyFormatter.toString(record.key())
                + " Timestamp: " + timestampFormat.format(record.timestamp())
                + " Data Length: " + record.value().length);
    }
}
