package de.azapps.kafkabackup.cli.formatters;

import java.nio.charset.StandardCharsets;

public class RawFormatter implements ByteFormatter {
    @Override
    public String toString(byte[] in) {
        return new String(in, StandardCharsets.UTF_8);
    }
}
