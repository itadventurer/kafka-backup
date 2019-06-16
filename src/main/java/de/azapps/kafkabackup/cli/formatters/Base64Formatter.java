package de.azapps.kafkabackup.cli.formatters;

import java.util.Base64;

public class Base64Formatter implements ByteFormatter {
    @Override
    public String toString(byte[] in) {
        Base64.Encoder encoder = Base64.getEncoder();
        return encoder.encodeToString(in);
    }
}
