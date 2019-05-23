package de.azapps.kafkabackup.common.partition;

import java.io.EOFException;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class PartitionIndex {
    private List<PartitionIndexEntry> index = new ArrayList<>();
    private FileOutputStream fileOutputStream;
    private FileInputStream fileInputStream;
    private int position = 0;
    private long latestStartOffset = -1;

    PartitionIndex(Path indexFile) throws IOException, IndexException {
        this.fileInputStream = new FileInputStream(indexFile.toFile());
        this.fileOutputStream = new FileOutputStream(indexFile.toFile(), true);
        fileInputStream.getChannel().position(0);
        while (true) {
            try {
                PartitionIndexEntry partitionIndexEntry = PartitionIndexEntry.fromStream(fileInputStream);
                if (partitionIndexEntry.startOffset() <= latestStartOffset) {
                    throw new IndexException("Offsets must be always increasing! There is something terribly wrong in your index! Got " + partitionIndexEntry.startOffset() + " expected an offset larger than " + latestStartOffset);
                }
                index.add(partitionIndexEntry);
                latestStartOffset = partitionIndexEntry.startOffset();
            } catch (EOFException e) {
                // reached End of File
                break;
            }
        }
    }

    void appendSegment(String segmentFile, long startOffset) throws IOException, IndexException {
        if (startOffset <= latestStartOffset) {
            throw new IndexException("Offsets must be always increasing! There is something terribly wrong in your index! Got " + startOffset + " expected an offset larger than " + latestStartOffset);
        }
        PartitionIndexEntry indexEntry = new PartitionIndexEntry(fileOutputStream, segmentFile, startOffset);
        index.add(indexEntry);
        latestStartOffset = startOffset;
    }

    Optional<PartitionIndexEntry> latestSegmentFile() {
        if(index.isEmpty()) {
            return Optional.empty();
        } else {
            return Optional.of(index.get(index.size() - 1));
        }
    }

    long latestStartOffset() {
        return latestStartOffset;
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

    long firstOffset() {
        return index.get(0).startOffset();
    }

    void seek(long offset) throws PartitionIndex.IndexException {
        int previousPosition = -1;
        for (int i = 0; i < index.size(); i++) {
            PartitionIndexEntry current = index.get(i);
            if (current.startOffset() > offset) {
                if (previousPosition >= 0) {
                    position = previousPosition;
                } else {
                    throw new PartitionIndex.IndexException("No Index file found matching the target index. Search for offset " + offset + ", smallest offset in index: " + current.startOffset());
                }
            } else {
                previousPosition = i;
            }
        }
    }

    boolean hasMoreData() {
        return position < index.size();
    }

    String readFileName() {
        if(!hasMoreData()) {
            throw new IndexOutOfBoundsException("No more data");
        }
        String fileName = index.get(position).filename();
        position++;
        return fileName;
    }
}
