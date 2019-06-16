package de.azapps.kafkabackup.cli.formatters;

public class RawFormatter implements ByteFormatter {
    @Override
    public String toString(byte[] in) {
        return new String(in);
    }
}
