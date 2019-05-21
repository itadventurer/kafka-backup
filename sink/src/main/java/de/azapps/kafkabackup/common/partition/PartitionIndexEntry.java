package de.azapps.kafkabackup.common.partition;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

/**
 * Format:
 * fileNameLength: int32
 * fileName: UTF8-String[fileNameLength]
 * startOffset: int64
 * [endOffset: int64]
 */
class PartitionIndexEntry {
    private String filename;
    private long startOffset;
    private Optional<Long> lastOffset;

    PartitionIndexEntry(OutputStream byteStream, String filename, long startOffset) throws IOException {
        this.filename = filename;
        this.startOffset = startOffset;
        this.lastOffset = Optional.empty();
        DataOutputStream stream = new DataOutputStream(byteStream);
        byte[] filenameBytes = filename.getBytes(StandardCharsets.UTF_8);
        stream.writeInt(filenameBytes.length);
        stream.write(filenameBytes);
        stream.writeLong(startOffset);
    }

    private PartitionIndexEntry(String filename, long startOffset, Optional<Long> lastOffset) {
        this.filename = filename;
        this.startOffset = startOffset;
        this.lastOffset = lastOffset;
    }

    long startOffset() {
        return startOffset;
    }

    long lastOffset() {
        return lastOffset.get();
    }

    void endSegment(OutputStream byteStream, long lastOffset) throws IOException {
        this.lastOffset = Optional.of(lastOffset);
        DataOutputStream stream = new DataOutputStream(byteStream);
        stream.writeLong(lastOffset);
    }

    boolean hasLastOffset() {
        return lastOffset.isPresent();
    }

    static PartitionIndexEntry fromStream(InputStream byteStream) throws IOException {
        DataInputStream stream = new DataInputStream(byteStream);
        int filenameLength = stream.readInt();
        byte[] filenameBytes = new byte[filenameLength];
        stream.read(filenameBytes);
        String filename = new String(filenameBytes, StandardCharsets.UTF_8);
        long startOffset = stream.readLong();
        Optional<Long> endOffset = Optional.empty();
        try {
            long endOffsetRaw = stream.readLong();
            endOffset = Optional.of(endOffsetRaw);
        } catch(EOFException e) {
            // do nothing. Just an EOF
        }
        return new PartitionIndexEntry(filename, startOffset, endOffset);
    }



}
