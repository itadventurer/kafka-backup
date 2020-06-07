package de.azapps.kafkabackup.common.segment;

import java.io.*;
import java.util.Objects;

/**
 * Format:
 * offset: int64
 * recordFilePosition: int64
 * recordLength: int64
 */
public class SegmentIndexEntry {
    private final long offset;
    private final long recordFilePosition;
    private final long recordByteLength;

    SegmentIndexEntry(long offset, long recordFilePosition, long recordByteLength) {
        this.offset = offset;
        this.recordFilePosition = recordFilePosition;
        this.recordByteLength = recordByteLength;
    }

    static SegmentIndexEntry fromStream(InputStream byteStream) throws IOException {
        DataInputStream stream = new DataInputStream(byteStream);
        long offset = stream.readLong();
        long recordFileOffset = stream.readLong();
        long recordByteLength = stream.readLong();
        return new SegmentIndexEntry(offset, recordFileOffset, recordByteLength);
    }

    public long getOffset() {
        return offset;
    }

    public long recordFilePosition() {
        return recordFilePosition;
    }

    public long recordByteLength() {
        return recordByteLength;
    }

    void writeToStream(OutputStream byteStream) throws IOException {
        DataOutputStream stream = new DataOutputStream(byteStream);
        stream.writeLong(offset);
        stream.writeLong(recordFilePosition);
        stream.writeLong(recordByteLength);
    }

    @Override
    public int hashCode() {
        return Objects.hash(offset, recordFilePosition, recordByteLength);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        SegmentIndexEntry that = (SegmentIndexEntry) o;

        return Objects.equals(getOffset(), that.getOffset())
                && Objects.equals(recordFilePosition(), that.recordFilePosition())
                && Objects.equals(recordByteLength(), that.recordByteLength());
    }

    @Override
    public String toString() {
        return String.format("SegmentIndexEntry{offset: %d, recordFilePosition: %d, recordByteLength: %d}",
                offset, recordFilePosition, recordByteLength);
    }
}
