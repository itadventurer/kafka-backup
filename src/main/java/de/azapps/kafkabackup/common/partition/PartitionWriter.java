package de.azapps.kafkabackup.common.partition;

import de.azapps.kafkabackup.common.record.Record;

import java.io.IOException;

public interface PartitionWriter {
    public void append(Record record) throws PartitionException;
    public void close() throws PartitionException;
    public void flush() throws PartitionException;
    // lastWrittenOffset
    // topic
    // partition
}
