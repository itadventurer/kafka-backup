package de.azapps.kafkabackup.common.partition;

import java.io.*;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class PartitionIndex {
    private List<PartitionIndexEntry> index = new ArrayList<>();
    private FileOutputStream fileOutputStream;
    private FileInputStream fileInputStream;
    private long lastValidRecordOffset = -1;

    PartitionIndex(Path indexFile) throws IOException, IndexException {
        this.fileInputStream = new FileInputStream(indexFile.toFile());
        this.fileOutputStream = new FileOutputStream(indexFile.toFile(), true);
        fileInputStream.getChannel().position(0);
        while (true) {
            try {
                PartitionIndexEntry partitionIndexEntry = PartitionIndexEntry.fromStream(fileInputStream);
                if (partitionIndexEntry.startOffset() <= lastValidRecordOffset) {
                    throw new IndexException("Offsets must be always increasing! There is something terribly wrong in your index!");
                }
                index.add(partitionIndexEntry);
                if(partitionIndexEntry.hasLastOffset()) {
                    lastValidRecordOffset = partitionIndexEntry.lastOffset();
                } else {
                    lastValidRecordOffset = partitionIndexEntry.startOffset();
                }
            } catch (EOFException e) {
                // reached End of File
                break;
            }
        }
    }

    void startSegment(String segmentFile, long startOffset) throws IOException {
        PartitionIndexEntry indexEntry = new PartitionIndexEntry(fileOutputStream, segmentFile, startOffset);
        index.add(indexEntry);
    }

    boolean hasOpenSegment() {
        if(index.isEmpty()) {
            return false;
        } else {
            PartitionIndexEntry indexEntry = index.get(index.size() - 1);
            return !indexEntry.hasLastOffset();
        }
    }

    void endSegment(long lastOffset) throws IndexException, IOException {
        if(!hasOpenSegment()) {
            throw new IndexException("Segment has already a last offset");
        }
        PartitionIndexEntry indexEntry = index.get(index.size() - 1);
        indexEntry.endSegment(fileOutputStream, lastOffset);
    }

    void close() throws IOException {
        fileInputStream.close();
        fileOutputStream.close();
    }

    void flush() throws IOException {
        fileOutputStream.flush();
    }

    public static class IndexException extends Exception {
        IndexException(String message) {
            super(message);
        }
    }

    long getLastValidRecordOffset() {
        return lastValidRecordOffset;
    }
}
