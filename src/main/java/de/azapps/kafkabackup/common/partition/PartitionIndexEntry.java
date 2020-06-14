package de.azapps.kafkabackup.common.partition;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * Format:
 * fileNameLength: int32
 * fileName: UTF8-String[fileNameLength]
 * startOffset: int64
 * [endOffset: int64]
 */
public class PartitionIndexEntry {
    private final String filename;
    private final long startOffset;

    PartitionIndexEntry(OutputStream byteStream, String filename, long startOffset) throws IOException {
        this.filename = filename;
        this.startOffset = startOffset;
        DataOutputStream stream = new DataOutputStream(byteStream);
        byte[] filenameBytes = filename.getBytes(StandardCharsets.UTF_8);
        stream.writeInt(filenameBytes.length);
        stream.write(filenameBytes);
        stream.writeLong(startOffset);
    }

    PartitionIndexEntry(String filename, long startOffset) {
        this.filename = filename;
        this.startOffset = startOffset;
    }

    static PartitionIndexEntry fromStream(InputStream byteStream) throws IOException {
        DataInputStream stream = new DataInputStream(byteStream);
        int filenameLength = stream.readInt();
        byte[] filenameBytes = new byte[filenameLength];
        int readBytes = stream.read(filenameBytes);
        if (readBytes != filenameLength) {
            throw new IOException(String.format("Expected to read %d bytes, got %d", filenameLength, readBytes));
        }
        String filename = new String(filenameBytes, StandardCharsets.UTF_8);
        long startOffset = stream.readLong();
        return new PartitionIndexEntry(filename, startOffset);
    }

    public long startOffset() {
        return startOffset;
    }

    public String filename() {
        return filename;
    }

    @Override
    public int hashCode() {
        return Objects.hash(filename, startOffset);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        PartitionIndexEntry that = (PartitionIndexEntry) o;

        return Objects.equals(filename(), that.filename())
                && Objects.equals(startOffset(), that.startOffset());
    }

    @Override
    public String toString() {
        return String.format("PartitionIndexEntry{filename: %s, startOffset: %d}",
                filename, startOffset);
    }
}
