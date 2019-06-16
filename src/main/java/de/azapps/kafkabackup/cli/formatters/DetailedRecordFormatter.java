package de.azapps.kafkabackup.cli.formatters;

import de.azapps.kafkabackup.common.record.Record;

import java.io.PrintStream;

public class DetailedRecordFormatter extends RecordFormatter {

    public DetailedRecordFormatter(ByteFormatter keyFormatter, ByteFormatter valueFormatter) {
        super(keyFormatter, valueFormatter);
    }

    @Override
    public void writeTo(Record record, PrintStream outputStream) {
        outputStream.println(keyFormatter.toString(record.key())
                + ", "
                + valueFormatter.toString(record.value()));
    }
}
