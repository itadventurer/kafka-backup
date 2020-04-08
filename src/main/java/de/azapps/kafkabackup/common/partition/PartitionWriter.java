package de.azapps.kafkabackup.common.partition;

import de.azapps.kafkabackup.common.record.Record;

public interface PartitionWriter {
    void append(Record record) throws PartitionException;
    void close() throws PartitionException;
    void flush() throws PartitionException;
    String topic();
    int partition();
}
