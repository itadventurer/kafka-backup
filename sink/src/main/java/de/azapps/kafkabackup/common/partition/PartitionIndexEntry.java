package de.azapps.kafkabackup.common.partition;

import java.io.*;
import java.nio.charset.StandardCharsets;

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

    PartitionIndexEntry(OutputStream byteStream, String filename, long startOffset) throws IOException {
        this.filename = filename;
        this.startOffset = startOffset;
        DataOutputStream stream = new DataOutputStream(byteStream);
        byte[] filenameBytes = filename.getBytes(StandardCharsets.UTF_8);
        stream.writeInt(filenameBytes.length);
        stream.write(filenameBytes);
        stream.writeLong(startOffset);
    }

    private PartitionIndexEntry(String filename, long startOffset) {
        this.filename = filename;
        this.startOffset = startOffset;
    }

    long startOffset() {
        return startOffset;
    }

    static PartitionIndexEntry fromStream(InputStream byteStream) throws IOException {
        DataInputStream stream = new DataInputStream(byteStream);
        int filenameLength = stream.readInt();
        byte[] filenameBytes = new byte[filenameLength];
        stream.read(filenameBytes);
        String filename = new String(filenameBytes, StandardCharsets.UTF_8);
        long startOffset = stream.readLong();
        return new PartitionIndexEntry(filename, startOffset);
    }



}
