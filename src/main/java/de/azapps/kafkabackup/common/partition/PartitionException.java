package de.azapps.kafkabackup.common.partition;


public class PartitionException extends Exception {
    public PartitionException(String message) {
        super(message);
    }
    public PartitionException(Throwable e) { super(e); }
    public PartitionException(String message, Throwable e) { super(message, e); }
}