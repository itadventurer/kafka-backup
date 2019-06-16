package de.azapps.kafkabackup.cli.formatters;

import de.azapps.kafkabackup.common.record.Record;

import java.io.PrintStream;

public abstract class RecordFormatter {
    ByteFormatter keyFormatter;
    ByteFormatter valueFormatter;

    RecordFormatter(ByteFormatter keyFormatter, ByteFormatter valueFormatter) {
        this.keyFormatter = keyFormatter;
        this.valueFormatter = valueFormatter;
    }

    public abstract void writeTo(Record record, PrintStream outputStream);
}
