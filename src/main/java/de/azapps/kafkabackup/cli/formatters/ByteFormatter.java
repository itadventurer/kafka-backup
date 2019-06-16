package de.azapps.kafkabackup.cli.formatters;

public interface ByteFormatter {
    String toString(byte[] in);
}
